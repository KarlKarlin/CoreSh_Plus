package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class InfluxMetricsProcessing {
    private static final Logger logger = LoggerFactory.getLogger(InfluxMetricsProcessing.class);

    private static final double DOWNSCALE_THRESHOLD_RATIO = 0.6;
    private static final double SCALE_UP_BUFFER_RATIO = 0.2;
    private static final int MIN_SCALE_UP_COOLDOWN = 1;
    private static final int MAX_SCALE_UP_COOLDOWN = 3;
    private static final int SCALE_DOWN_COOLDOWN = 5;
    private static final double SCALE_UP_AGGRESSION = 1.2;

    private final MetricsProcessing metricsProcessing;
    private final RedisService redisService;
    private final ConfigProcessing configProcessing;
    private final DockerSwarmService dockerSwarmService;

    private final Map<String, List<MetricAnalysis>> serviceMetrics = new ConcurrentHashMap<>();

    private final Map<String, Long> lastScaleTime = new ConcurrentHashMap<>();

    public InfluxMetricsProcessing(MetricsProcessing metricsProcessing, RedisService redisService,
                                   ConfigProcessing configProcessing, DockerSwarmService dockerSwarmService) {
        this.metricsProcessing = metricsProcessing;
        this.redisService = redisService;
        this.configProcessing = configProcessing;
        this.dockerSwarmService = dockerSwarmService;
    }

    public void handleMetric(String measurement, String field, String serviceName, int minutes) {
        try {
            List<Double> metricsValues = metricsProcessing.getMetricsInTimeWindow(
                    measurement, field, serviceName, minutes);

            if (metricsValues.isEmpty()) {
                logger.warn("No metrics found for {}:{} in service {}", measurement, field, serviceName);
                return;
            }

            MetricAnalysis analysis = analyzeMetrics(measurement, field, serviceName, metricsValues);
            checkForMetricSpike(analysis);
            checkLoadAndScale(analysis);

        } catch (Exception e) {
            logger.error("Error processing metric {}:{} for service {}: {}",
                    measurement, field, serviceName, e.getMessage(), e);
        }
    }

    private MetricAnalysis analyzeMetrics(String measurement, String field,
                                          String serviceName, List<Double> values) {
        double currentLoad = calculateStableMetricValue(values);
        double previousPeak = redisService.getBiggestValueForMetric(field, serviceName);
        int percentageIncrease = previousPeak > 0 ?
                (int) (((currentLoad - previousPeak) / previousPeak) * 100) : 0;

        int currentReplicas = dockerSwarmService.getCurrentReplicasForService(serviceName);
        double threshold = getMetricThreshold(measurement + "." + field);
        double currentLoadPerReplica = currentReplicas > 0 ?
                currentLoad / currentReplicas : currentLoad;

        return new MetricAnalysis(
                measurement,
                field,
                serviceName,
                currentLoad,
                previousPeak,
                percentageIncrease,
                currentReplicas,
                threshold,
                currentLoadPerReplica
        );
    }

    private double getMetricThreshold(String metric) {
        return configProcessing.getMetricCriticalThreshold(metric);
    }

    private double calculateStableMetricValue(List<Double> values) {
        if (values.isEmpty()) return 0.0;
        if (values.size() <= 3) {
            return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
        values.sort(Double::compare);
        return values.get(values.size() / 2);
    }

    private void checkForMetricSpike(MetricAnalysis analysis) {
        if (analysis.currentPeak() > analysis.previousPeak()) {
            redisService.saveBiggestValueForMetric(
                    analysis.currentPeak(),
                    analysis.measurement(),
                    analysis.serviceName());

            int spikeThreshold = configProcessing.getSpikeNotificationThreshold();
            if (analysis.percentageIncrease() > spikeThreshold) {
                String message = String.format("""
                    *Service:* %s
                    *Metric Spike Detected:*
                    *Metric:* %s
                    *Current Load:* %.2f
                    *Previous Peak:* %.2f
                    *Increase:* %d%%
                    *Threshold:* %d%%
                    *Replicas:* %d
                    """,
                        analysis.serviceName(),
                        analysis.measurement(),
                        analysis.currentPeak(),
                        analysis.previousPeak(),
                        analysis.percentageIncrease(),
                        spikeThreshold,
                        analysis.currentReplicas());

                redisService.commentProblem(analysis.serviceName(), message, "critical");
            }
        }
    }

    private void checkLoadAndScale(MetricAnalysis analysis) {

        serviceMetrics.computeIfAbsent(analysis.serviceName(), k -> new ArrayList<>())
                .add(analysis);

        List<MetricAnalysis> metrics = serviceMetrics.get(analysis.serviceName());

        if (metrics.size() < configProcessing.getMonitoredMetrics().size()) {
            return;
        }

        int currentReplicas = dockerSwarmService.getCurrentReplicasForService(analysis.serviceName());
        analysis = new MetricAnalysis(
                analysis.measurement(),
                analysis.field(),
                analysis.serviceName(),
                analysis.currentPeak(),
                analysis.previousPeak(),
                analysis.percentageIncrease(),
                currentReplicas,
                analysis.threshold(),
                analysis.currentPeak() / currentReplicas
        );

        double warningThreshold = configProcessing.getMetricWarningThreshold(
                analysis.measurement() + "." + analysis.field());

        MetricAnalysis worstMetric = metrics.stream()
                .max(Comparator.comparingDouble(m -> m.currentLoadPerReplica() / m.threshold()))
                .orElse(analysis);

        double maxLoadPerReplica = worstMetric.currentLoadPerReplica();

        logger.debug("Service {} - Worst Metric: {}, Load: {}/{} ({}%), Replicas: {}/{}",
                analysis.serviceName(),
                worstMetric.measurement(),
                maxLoadPerReplica,
                worstMetric.threshold(),
                (maxLoadPerReplica/worstMetric.threshold())*100,
                currentReplicas,
                configProcessing.getMaxReplicasForService(analysis.serviceName()));

        if (analysis.currentLoadPerReplica() > warningThreshold &&
                analysis.currentLoadPerReplica() < analysis.threshold()) {
            sendWarning(analysis, warningThreshold);
        }

        if (isInCooldown(analysis.serviceName())) {
            logger.debug("Service {} in cooldown", analysis.serviceName());
            serviceMetrics.remove(analysis.serviceName());
            return;
        }

        if (maxLoadPerReplica > worstMetric.threshold()) {
            int newReplicas = calculateRequiredReplicas(metrics, true, currentReplicas);
            if (newReplicas > currentReplicas) {
                logger.info("SCALE UP {}: {} -> {} (metric: {}, load {}/replica, threshold {})",
                        analysis.serviceName(), currentReplicas, newReplicas,
                        worstMetric.measurement(),
                        maxLoadPerReplica, worstMetric.threshold());

                scaleService(worstMetric, newReplicas, "UP");
                serviceMetrics.remove(analysis.serviceName());
                return;
            }
        }

        boolean allMetricsSafe = metrics.stream()
                .allMatch(m -> m.currentLoadPerReplica() < (m.threshold() * DOWNSCALE_THRESHOLD_RATIO));

        if (allMetricsSafe && currentReplicas > 1) {
            int newReplicas = calculateRequiredReplicas(metrics, false, currentReplicas);
            if (newReplicas < currentReplicas) {
                logger.info("SCALE DOWN {}: {} -> {} (all metrics safe)",
                        analysis.serviceName(), currentReplicas, newReplicas);

                scaleService(analysis, newReplicas, "DOWN");
            }
        }

        serviceMetrics.remove(analysis.serviceName());
    }

    private int calculateRequiredReplicas(List<MetricAnalysis> metrics, boolean scaleUp, int currentReplicas) {
        if (scaleUp) {
            MetricAnalysis worstMetric = metrics.stream()
                    .max(Comparator.comparingDouble(m -> m.currentLoadPerReplica() / m.threshold()))
                    .orElse(metrics.get(0));

            int baseReplicas = (int) Math.ceil(worstMetric.currentPeak() / worstMetric.threshold());

            double overloadRatio = worstMetric.currentLoadPerReplica() / worstMetric.threshold();
            double bufferMultiplier = SCALE_UP_BUFFER_RATIO * Math.min(2.0, overloadRatio);
            int bufferReplicas = (int) Math.ceil(baseReplicas * bufferMultiplier);

            int maxReplicas = configProcessing.getMaxReplicasForService(worstMetric.serviceName());
            int calculatedReplicas = Math.min(baseReplicas + bufferReplicas, maxReplicas);

            return Math.max(currentReplicas + 1, (int)(calculatedReplicas * SCALE_UP_AGGRESSION));
        } else {

            MetricAnalysis bestMetric = metrics.stream()
                    .min(Comparator.comparingDouble(m -> m.currentLoadPerReplica() / m.threshold()))
                    .orElse(metrics.get(0));

            int suggestedReplicas = (int) Math.floor(bestMetric.currentPeak() /
                    (bestMetric.threshold() * DOWNSCALE_THRESHOLD_RATIO * 0.9));

            return Math.max(
                    configProcessing.getMinReplicasForService(bestMetric.serviceName()),
                    suggestedReplicas
            );
        }
    }

    private boolean isInCooldown(String serviceName) {
        Long lastScale = lastScaleTime.get(serviceName);
        if (lastScale == null) return false;

        int cooldownMinutes = redisService.exists("scalingCooldown:" + serviceName) ?
                SCALE_DOWN_COOLDOWN :
                (System.currentTimeMillis() - lastScale) < (MIN_SCALE_UP_COOLDOWN * 60 * 1000) ?
                        MIN_SCALE_UP_COOLDOWN : MAX_SCALE_UP_COOLDOWN;

        return (System.currentTimeMillis() - lastScale) < (cooldownMinutes * 60 * 1000);
    }

    private void sendWarning(MetricAnalysis analysis, double warningThreshold) {
        String message = String.format("""
            **Service:** %s
            **Metric Warning:**
            **Trigger Metric:** %s
            **Current Load/Replica:** %.2f
            **Warning Threshold:** %.2f
            **Critical Threshold:** %.2f
            **Replicas:** %d
            """,
                analysis.serviceName(),
                analysis.measurement(),
                analysis.currentLoadPerReplica(),
                warningThreshold,
                analysis.threshold(),
                analysis.currentReplicas());

        redisService.commentProblem(analysis.serviceName(), message, "warn");
    }

    private void scaleService(MetricAnalysis analysis, int newReplicas, String direction) {

        int protectedReplicas = redisService.getProtectedReplicasCount(analysis.serviceName());
        if (protectedReplicas > 0 && newReplicas < protectedReplicas) {
            if ("DOWN".equals(direction) && analysis.currentReplicas() > protectedReplicas) {
                newReplicas = protectedReplicas;
                String message = String.format("""
                    *Service:* %s
                    *Scale Down Limited:*
                    *Current:* %d
                    *Protected:* %d
                    """,
                        analysis.serviceName(),
                        analysis.currentReplicas(),
                        protectedReplicas);
                redisService.commentProblem(analysis.serviceName(), message, "warn");
            } else {
                return;
            }
        }

        int maxReplicas = configProcessing.getMaxReplicasForService(analysis.serviceName());
        int minReplicas = configProcessing.getMinReplicasForService(analysis.serviceName());
        newReplicas = Math.max(minReplicas, Math.min(maxReplicas, newReplicas));

        if (newReplicas == analysis.currentReplicas()) {
            return;
        }

        String message = String.format("""
            *Service:* %s
            *Scaling %s:*
            *Trigger Metric:* %s
            *From:* %d
            *To:* %d
            *Load/Replica:* %.2f
            *Threshold:* %.2f
            """,
                analysis.serviceName(),
                direction,
                analysis.measurement(),
                analysis.currentReplicas(),
                newReplicas,
                analysis.currentLoadPerReplica(),
                analysis.threshold());

        redisService.commentProblem(analysis.serviceName(), message, "info");
        dockerSwarmService.scaleUpService(analysis.serviceName(), newReplicas);

        int cooldownMinutes = "UP".equals(direction) ?
                calculateScaleUpCooldown(analysis.currentLoadPerReplica(), analysis.threshold()) :
                SCALE_DOWN_COOLDOWN;

        redisService.setWithExpiry("scalingCooldown:" + analysis.serviceName(),
                "cooldown", cooldownMinutes);
        lastScaleTime.put(analysis.serviceName(), System.currentTimeMillis());

        int actualReplicas = dockerSwarmService.getCurrentReplicasForService(analysis.serviceName());
        String resultMessage = String.format("""
            *Service:* %s
            *Scale Result:*
            *Requested:* %d
            *Actual:* %d
            *Cooldown:* %d min
            *Trigger Metric:* %s
            *Current Load:* %.2f
            """,
                analysis.serviceName(),
                newReplicas,
                actualReplicas,
                cooldownMinutes,
                analysis.measurement(),
                analysis.currentPeak() / actualReplicas);

        redisService.commentProblem(analysis.serviceName(), resultMessage, "info");
    }

    private int calculateScaleUpCooldown(double currentLoad, double threshold) {
        double overloadRatio = currentLoad / threshold;
        if (overloadRatio > 2.0) return MIN_SCALE_UP_COOLDOWN;
        if (overloadRatio > 1.5) return 2;
        return MAX_SCALE_UP_COOLDOWN;
    }

    public double getMaxMetricValueForLastHour(String measurement, String field, String serviceName) {
        try {
            List<Double> metricsValues = metricsProcessing.getMetricsInTimeWindow(
                    measurement, field, serviceName, 60);

            if (metricsValues.isEmpty()) {
                logger.warn("No metrics found for {}:{} in service {} in last hour",
                        measurement, field, serviceName);
                return Double.NaN;
            }

            double maxValue = metricsValues.stream()
                    .mapToDouble(Double::doubleValue)
                    .max()
                    .getAsDouble();

            logger.debug("Max value for {}:{} in {}: {}",
                    measurement, field, serviceName, maxValue);

            return maxValue;

        } catch (Exception e) {
            logger.error("Error getting max metric for {}:{} in {}: {}",
                    measurement, field, serviceName, e.getMessage(), e);
            throw new RuntimeException("Failed to get max metric value", e);
        }
    }

    private record MetricAnalysis(
            String measurement,
            String field,
            String serviceName,
            double currentPeak,
            double previousPeak,
            int percentageIncrease,
            int currentReplicas,
            double threshold,
            double currentLoadPerReplica
    ) {}
}