package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class InfluxMetricsProcessing {
    private static final Logger logger = LoggerFactory.getLogger(InfluxMetricsProcessing.class);

    private static final double DOWNSCALE_THRESHOLD_RATIO = 0.3;
    private static final double BUFFER_REPLICAS_RATIO = 0.2;

    private final MetricsProcessing metricsProcessing;
    private final RedisService redisService;
    private final ConfigProcessing configProcessing;
    private final DockerSwarmService dockerSwarmService;

    public InfluxMetricsProcessing(MetricsProcessing metricsProcessing, RedisService redisService,
                                   ConfigProcessing configProcessing, DockerSwarmService dockerSwarmService) {
        this.metricsProcessing = metricsProcessing;
        this.redisService = redisService;
        this.configProcessing = configProcessing;
        this.dockerSwarmService = dockerSwarmService;
    }

    public void handleMetric(String measurement, String field, String serviceName, int minutes) {
        try {

            List<Double> metricsValues = metricsProcessing.getMetricsInTimeWindow(measurement, field, serviceName, minutes);

            if (metricsValues.isEmpty()) {
                logger.warn("No metrics found for {}:{} in service {}", measurement, field, serviceName);
                return;
            }

            MetricAnalysis analysis = analyzeMetrics(measurement, field, serviceName, metricsValues);
            checkForMetricSpike(analysis);
            checkLoadAndScale(analysis);

        } catch (Exception e) {
            logger.error("Error processing metric {}:{} for service {}", measurement, field, serviceName, e);
        }
    }

    private MetricAnalysis analyzeMetrics(String measurement, String field, String serviceName, List<Double> values) {
        double currentLoad = calculateStableMetricValue(values);
        double previousPeak = redisService.getBiggestValueForMetric(field, serviceName);
        int percentageIncrease = previousPeak > 0 ?
                (int) (((currentLoad - previousPeak) / previousPeak) * 100) : 0;

        int currentReplicas = dockerSwarmService.getCurrentReplicasForService(serviceName);

        double threshold = getMetricThreshold(field);

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
        if (values.size() <= 3) {
            return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }

        values.sort(Double::compare);
        int index = values.size() / 2;
        return values.get(index);
    }

    private void checkForMetricSpike(MetricAnalysis analysis) {
        if (analysis.currentPeak() > analysis.previousPeak()) {
            redisService.clearBiggestValueForMetric(analysis.measurement, analysis.serviceName);
            redisService.saveBiggestValueForMetric(analysis.currentPeak(), analysis.measurement, analysis.serviceName());

            int spikeNotificationThreshold = configProcessing.getSpikeNotificationThreshold();
            if (analysis.percentageIncrease() > spikeNotificationThreshold) {

                String message = String.format("""
                        *Service:* %s
                        *Metric:* %s
                        *Current Load:* %.2f
                        *Previous Peak:* %.2f
                        *Increase:* %d%%
                        *Threshold:* %d%%
                        *Current Replicas:* %d
                        """,
                        analysis.serviceName(),
                        analysis.field(),
                        analysis.currentPeak(),
                        analysis.previousPeak(),
                        analysis.percentageIncrease(),
                        spikeNotificationThreshold,
                        analysis.currentReplicas());

                redisService.commentProblem(analysis.serviceName, message, "critical");
            }
        }
    }

    private void checkLoadAndScale(MetricAnalysis analysis) {
        double downscaleThreshold = analysis.threshold() * DOWNSCALE_THRESHOLD_RATIO;
        double warningThreshold = configProcessing.getMetricWarningThreshold(analysis.measurement + "." + analysis.field);

        if (analysis.currentLoadPerReplica() > warningThreshold && analysis.currentLoadPerReplica() < analysis.threshold()) {
            String message = String.format("""
                    **Service:** %s
                    **Trigger Metric:** %s.%s
                    **Current Load/Replica:** %.2f
                    **Warning Threshold:** %.2f
                    **Critical Threshold:** %.2f
                    **Warning:** Metric is approaching critical threshold
                    """,
                    analysis.serviceName(),
                    analysis.measurement(),
                    analysis.field(),
                    analysis.currentLoadPerReplica(),
                    warningThreshold,
                    analysis.threshold());

            redisService.commentProblem(analysis.serviceName(), message, "warn");
        }

        if (analysis.currentLoadPerReplica() > analysis.threshold()) {
            int newReplicas = calculateNewReplicaCount(analysis);
            if (newReplicas > analysis.currentReplicas()) {
                scaleService(analysis, newReplicas, "UP");
            }
        }
        else if (analysis.currentLoadPerReplica() < downscaleThreshold && analysis.currentReplicas() > 1) {
            int newReplicas = calculateReducedReplicaCount(analysis);
            if (newReplicas < analysis.currentReplicas()) {
                scaleService(analysis, newReplicas, "DOWN");
            }
        }
    }

    private int calculateNewReplicaCount(MetricAnalysis analysis) {
        int minReplicas = (int) Math.ceil(analysis.currentPeak() / analysis.threshold());
        int bufferReplicas = Math.max(configProcessing.getMinReplicasForService(analysis.serviceName),
                (int) Math.ceil(minReplicas * BUFFER_REPLICAS_RATIO));
        int maxReplicas = configProcessing.getMaxReplicasForService(analysis.serviceName());

        return Math.min(minReplicas + bufferReplicas, maxReplicas);
    }

    private int calculateReducedReplicaCount(MetricAnalysis analysis) {
        int suggestedReplicas = (int) Math.floor(analysis.currentPeak() /
                (analysis.threshold() * DOWNSCALE_THRESHOLD_RATIO));
        return Math.max(1, suggestedReplicas);
    }

    private void scaleService(MetricAnalysis analysis, int newReplicas, String direction) {

        if (redisService.getProtectedReplicasCount(analysis.serviceName) > newReplicas) {
            if (analysis.currentReplicas() > redisService.getProtectedReplicasCount(analysis.serviceName)) {

                dockerSwarmService.scaleUpService(analysis.serviceName, redisService.getProtectedReplicasCount(analysis.serviceName));

                String message = String.format("""
                *Service:* %s
                *Direction:* DOWN
                *Current Replicas:* %d
                *Proposed Replicas:* %d
                """,
                        analysis.serviceName(),
                        analysis.currentReplicas(),
                        redisService.getProtectedReplicasCount(analysis.serviceName));

                redisService.commentProblem(analysis.serviceName, message, "warn");
            }
            return;
        }

        if (redisService.isCooldown(analysis.serviceName())) return;

        int maxReplicas = configProcessing.getMaxReplicasForService(analysis.serviceName());

        if (maxReplicas < newReplicas) newReplicas = maxReplicas;

        String message = String.format("""
                *Service:* %s
                *Direction:* %s
                *Current Replicas:* %d
                *Proposed Replicas:* %d
                *Load/Replica:* %.2f
                *Threshold:* %.2f
                *Trigger Metric:* %s
                """,
                analysis.serviceName(),
                direction,
                analysis.currentReplicas(),
                newReplicas,
                analysis.currentLoadPerReplica(),
                analysis.threshold(),
                analysis.field());

        redisService.commentProblem(analysis.serviceName(), message, "warn");

        dockerSwarmService.scaleUpService(analysis.serviceName(), newReplicas);

        int actualNewReplicas = dockerSwarmService.getCurrentReplicasForService(analysis.serviceName());
        double newLoadPerReplica = analysis.currentPeak() / actualNewReplicas;

        String message1 = String.format("""
                *Service:* %s
                *Previous Replicas:* %d
                *New Replica Count:* %d
                *Load/Replica After Scaling:* %.2f
                """,
                analysis.serviceName(),
                analysis.currentReplicas(),
                actualNewReplicas,
                newLoadPerReplica);

        redisService.commentProblem(analysis.serviceName(), message1, "warn");

        redisService.setCooldown(analysis.serviceName, analysis.measurement + "." + analysis.field);
    }

    public double getMaxMetricValueForLastHour(String measurement, String field, String serviceName) {
        try {

            List<Double> metricsValues = metricsProcessing.getMetricsInTimeWindow(
                    measurement,
                    field,
                    serviceName,
                    60
            );

            if (metricsValues.isEmpty()) {
                logger.warn("No metrics found for {}:{} in service {} for last hour",
                        measurement, field, serviceName);
                return 0.0;
            }

            return metricsValues.stream()
                    .mapToDouble(Double::doubleValue)
                    .max()
                    .orElse(0.0);

        } catch (Exception e) {
            logger.error("Error getting max metric value for {}:{} in service {}",
                    measurement, field, serviceName, e);
            return 0.0;
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