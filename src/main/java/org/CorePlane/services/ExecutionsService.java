package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ExecutionsService {
    private static final Logger logger = LoggerFactory.getLogger(ExecutionsService.class);

    private final ConfigProcessing configProcessing;
    private final WriteStateService writeStateService;
    private final RedisService redisService;
    private final InfluxMetricsProcessing InfluxMetricsProcessing;
    private final JaegerTracesProcessing jaegerTracesProcessing;
    private final DockerSwarmService dockerSwarmService;
    private final MemoryAnalyzer memoryAnalyzer;
    private final MetricsProcessing MetricProcessing;
    private final AttacksAnalyzer attacksAnalyzer;

    public ExecutionsService(ConfigProcessing configProcessing,
                             WriteStateService writeStateService,
                             RedisService redisService,
                             InfluxMetricsProcessing InfluxMetricsProcessing,
                             JaegerTracesProcessing jaegerTracesProcessing,
                             DockerSwarmService dockerSwarmService,
                             MemoryAnalyzer memoryAnalyzer, MetricsProcessing MetricProcessing, AttacksAnalyzer attacksAnalyzer) {
        this.configProcessing = configProcessing;
        this.writeStateService = writeStateService;
        this.redisService = redisService;
        this.InfluxMetricsProcessing = InfluxMetricsProcessing;
        this.jaegerTracesProcessing = jaegerTracesProcessing;
        this.dockerSwarmService = dockerSwarmService;
        this.memoryAnalyzer = memoryAnalyzer;
        this.MetricProcessing = MetricProcessing;
        this.attacksAnalyzer = attacksAnalyzer;
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void restartAllCrashedPods() {

        List<String> services = configProcessing.getDockerServices();

        for (String service : services) {
            if(dockerSwarmService.restartCrashedContainers(service)) {

                String message = String.format("Removing crashed pod %s", service);

                redisService.commentProblem(service, message, "notification");
            }
        }
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void executeStateWrite() {
        try {
            List<String> services = configProcessing.getDockerServices();
            if (services.isEmpty()) {
                logger.warn("No Docker services found in configuration");
                return;
            }

            for (String service : services) {
                try {
                    if (redisService.hasLock(service)) {
                        logger.debug("Service {} is already being processed by another instance", service);
                        continue;
                    }

                    writeStateService.checkAndWriteState(service);
                } catch (Exception e) {
                    logger.error("Failed to write state for service {}", service, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error in state write execution", e);
        }
    }

    @Scheduled(fixedDelayString = "10000")
    public void checkLastUpdate() {
        try {
            List<String> services = configProcessing.getDockerServices();
            if (services.isEmpty()) {
                logger.warn("No Docker services found in configuration");
                return;
            }

            for (String service : services) {
                try {

                    redisService.trackServiceDeployment(service);

                } catch (Exception e) {
                    logger.error("Failed to save last update for service {}", service, e);
                }
            }
        } catch (Exception e) {
            logger.error("Error in last update check execution", e);
        }
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void handleAllMetricsAutomatically() {
        try {
            List<String> services = configProcessing.getDockerServices();
            List<String> metricsToMonitor = configProcessing.getMonitoredMetrics();

            if (services.isEmpty() || metricsToMonitor.isEmpty()) {
                logger.warn("No services or metrics configured for monitoring");
                return;
            }

            for (String service : services) {
                for (String metric : metricsToMonitor) {
                    try {
                        processMetric(service, metric);
                    } catch (Exception e) {
                        logger.error("Failed to process metric {} for service {}", metric, service, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in metrics processing execution", e);
        }
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void tracerProcessing() {

        try {

            List<String> services = configProcessing.getServicesForTracer();

            if (services.isEmpty()) {
                logger.warn("No Jaeger services found in configuration");
                return;
            }

            for (String service : services) {
                jaegerTracesProcessing.fixErrorRate(service);
            }

        }catch (Exception e){
            logger.error("Error in tracer processing execution", e);
        }
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void analyzeServiceTracesEL() {
        jaegerTracesProcessing.analyzeServiceTraces();
    }

    @Scheduled(cron = "0 0 * * * *")
    public void predictAndScale() {
        if (!configProcessing.isAutoPredictsAvailable()) {
            logger.debug("Auto-scaling predictions are disabled in configuration");
            return;
        }

        configProcessing.getDockerServices().forEach(service -> {
            try {
                double currentLoad = InfluxMetricsProcessing.getMaxMetricValueForLastHour(
                        "cpu", "usage_percent", service);

                redisService.addLtForDay(service, currentLoad);
                List<Double> hourlyLoads = redisService.getLtForDay(service);

                if (hourlyLoads.size() >= 24) {
                    WorkloadPredictor predictor = new StatisticalWorkloadPredictor(
                            redisService, configProcessing, dockerSwarmService);

                    int recommended = (int) predictor.predictRequiredInstances(service);
                    int current = dockerSwarmService.getCurrentReplicasForService(service);

                    double avgLoad = hourlyLoads.stream()
                            .mapToDouble(d -> d)
                            .average()
                            .orElse(0);
                    redisService.setWLForService(service, avgLoad);

                    double maxLoad = Collections.max(hourlyLoads);
                    double p95Load = calculatePercentile(hourlyLoads, 0.95);

                    logger.info("Service {} load - Avg: {:.1f}%, Max: {:.1f}%, P95: {:.1f}%",
                            service, avgLoad, maxLoad, p95Load);

                    if (Math.abs(recommended - current) >= 1) {
                        dockerSwarmService.scaleUpService(service, recommended);
                        redisService.setProtectedReplicasCount(recommended, service);

                        logger.info("Scaled {} from {} to {} replicas",
                                service, current, recommended);

                        sendScalingNotification(service, current, recommended,
                                avgLoad, maxLoad, p95Load);
                    }

                    redisService.clearLtForDay(service);
                }
            } catch (Exception e) {
                logger.error("Scaling failed for {}", service, e);
                redisService.commentProblem(service,
                        "Scaling error: " + e.getMessage(), "error");
            }
        });
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void analyzeMemory() {
        List<String> services = configProcessing.getDockerServices();
        int windowMinutes = configProcessing.getMetricWindowMinutes("memory.used_percent");

        for (String service : services) {

            if (redisService.isCooldownN(service, "memory.used_percent", "frozen2")) return;

            MemoryAnalyzer.MemoryAnalysisResult result = memoryAnalyzer.analyzeMemoryPerPod(service, windowMinutes);

            if (result.possibleLeak || result.possibleBloat) {

                String message = String.format(
                        """
                        **Service:** %s
                        **Analysis Window:** %d minutes
                               
                        **Memory Analysis Results:**
                        ▸ Area Ratio: %.2f %s
                        ▸ Standard Deviation: %.2f%% %s
                               
                        **Thresholds:**
                        ▸ Leak Warning: Ratio > %.2f
                        ▸ Bloat Warning: σ > %.2f%%
                               
                        **Diagnosis:** %s""",
                        service,
                        windowMinutes,
                        result.areaRatio,
                        result.possibleLeak ? "(LEAK DETECTED)" : "",
                        result.standardDeviation,
                        result.possibleBloat ? "(BLOAT DETECTED)" : "",
                        MemoryAnalyzer.LEAK_THRESHOLD,
                        MemoryAnalyzer.BLOAT_THRESHOLD,
                        generateDiagnosisMessage(result)
                );

                redisService.commentProblem(service, message, "critical");

                dockerSwarmService.restartAllPodsForService(service);

                redisService.setCooldownN(service, "memory.used_percent", "frozen2");
            }
        }
    }

    @Scheduled(fixedDelayString = "10000")
    public void clearOldProblems() {
        redisService.cleanupOldProblems(7 * 24 * 60 * 60 * 1000);
    }

    @Scheduled(fixedDelayString = "${schedule.checks.interval}")
    public void getLastErrorLogs() {

        if(!configProcessing.isLogTracker()) return;

        List<String> services = configProcessing.getDockerServices();

        if(redisService.exists("logs_error_cooldown")) return;

        for (String service : services) {
            Map<String, List<String>> errorLogs = dockerSwarmService.identifyErrorTasks(
                    service,
                    System.currentTimeMillis(),
                    configProcessing.getWindowMinutesForLogs()
            );

            if (!errorLogs.isEmpty()) {

                Map.Entry<String, List<String>> firstError = errorLogs.entrySet().iterator().next();
                String errorDescription = firstError.getKey();
                String fullContext = firstError.getValue().get(0);


                if (fullContext.length() > 500) {
                    fullContext = fullContext.substring(0, 500) + "... [truncated]";
                }

                String message = String.format("""
                    **Service:** %s
                    **Error:** %s
                    **Full context:** %s
                    **Timestamp:** %s
                    **Anomaly detected in service logs**
                    """,
                        service,
                        errorDescription,
                        fullContext,
                        Instant.now().toString()
                );

                redisService.commentProblem(service, message, "notification");

                redisService.setWithExpiry("logs_error_cooldown", "frozen", configProcessing.getWindowMinutesForLogs());
            }
        }
    }

    @Scheduled(fixedDelayString = "300000")
    public void executeCleanupPerHost() {

        try {
            List<String> services = configProcessing.getDockerServices();
            List<String> metricsToMonitor = configProcessing.getMonitoredMetrics();

            if (services.isEmpty() || metricsToMonitor.isEmpty()) {
                logger.warn("No services or metrics configured for monitoring");
                return;
            }

            for (String service : services) {
                for (String metric : metricsToMonitor) {
                    try {
                        cleanupEmptyHosts(service, metric);
                    } catch (Exception e) {
                        logger.error("Failed to process metric {} for service {}", metric, service, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error in host cleanup execution", e);
        }

    }

    @Scheduled(fixedDelayString = "60000")
    public void monitorAndAlertForAttacks() {
        if (!configProcessing.isAttacksAnalysisAvailable()) {
            logger.debug("Attack analysis is currently disabled in configuration");
            return;
        }

        logger.info("Starting attack monitoring cycle");
        Instant monitoringStart = Instant.now();

        configProcessing.getDockerServices().forEach(service -> {
            try {

                logger.debug("Analyzing service {} for potential attacks", service);
                Map<String, Object> attackResults = attacksAnalyzer.analyzeForAttacks(service);
                processAttackResults(service, attackResults);

            } catch (Exception e) {
                logger.error("Error while monitoring service {} for attacks", service, e);
                redisService.commentProblem(service,
                        "Attack monitoring failed: " + e.getMessage(),
                        "error");
            }
        });

        logger.info("Completed attack monitoring cycle in {} ms",
                Duration.between(monitoringStart, Instant.now()).toMillis());
    }


    @Scheduled(fixedDelayString = "300000")
    public void predictAndScaleShortTerm() {
        if (!configProcessing.isAutoPredictsAvailable()) {
            logger.debug("Auto-scaling predictions are disabled");
            return;
        }

        configProcessing.getDockerServices().forEach(service -> {
            try {
                int currentPods = dockerSwarmService.getCurrentReplicasForService(service);
                double podCapacity = configProcessing.getMetricCriticalThreshold("cpu.usage_percent");

                int requiredPods = MetricProcessing.predictRequiredPods(
                        "cpu",
                        "usage_percent",
                        service,
                        currentPods,
                        podCapacity,
                        configProcessing.getMinReplicasForService(service),
                        configProcessing.getMaxReplicasForService(service),
                        5
                );

                if (shouldScale(currentPods, requiredPods)) {
                    scaleService(service, currentPods, requiredPods);
                }
            } catch (Exception e) {
                logger.error("Scaling failed for {}", service, e);
                redisService.commentProblem(service,
                        "Scaling error: " + e.getMessage(), "error");
            }
        });
    }

    private boolean shouldScale(int current, int recommended) {

        int minChange = Math.max(1, (int) Math.ceil(current * 0.25));
        return Math.abs(recommended - current) >= minChange;
    }

    private void scaleService(String service, int current, int target) {
        try {
            dockerSwarmService.scaleUpService(service, target);
            redisService.setProtectedReplicasCount(target, service);

            logger.info("Scaled {}: {} -> {} pods", service, current, target);

            String message = String.format(
                    "Service scaled: %s\n" +
                            "Replicas: %d → %d\n" +
                            "Timestamp: %s\n" +
                            "Decision: %s",
                    service, current, target, Instant.now(),
                    target > current ? "SCALE UP" : "SCALE DOWN");
            redisService.commentProblem(service, message, "scaling");
        } catch (Exception e) {
            logger.error("Failed to scale {} to {} pods", service, target, e);
            throw e;
        }
    }

    private void processAttackResults(String service, Map<String, Object> attackResults) {

        processAttackResult(service, "ddos_volumetric", attackResults,
                (result) -> {
                    double currentRate = (double) result.getOrDefault("current_median_rate", 0.0);
                    double threshold = (double) result.getOrDefault("threshold", 0.0);
                    double exceedanceRatio = (double) result.getOrDefault("exceedance_ratio", 0.0);
                    double trendSlope = (double) result.getOrDefault("trend_slope", 0.0);

                    String severity = exceedanceRatio > 0.9 || trendSlope > 1.0 ? "critical" : "warning";

                    return new AlertMessage(
                            String.format(
                                    "DDoS alert on %s: Median rate %.2f (threshold: %.2f), " +
                                            "Exceedance: %.1f%%, Trend: %.2f",
                                    service, currentRate, threshold, exceedanceRatio * 100, trendSlope
                            ),
                            severity,
                            Map.of(
                                    "current_rate", currentRate,
                                    "threshold", threshold,
                                    "exceedance_ratio", exceedanceRatio,
                                    "trend_slope", trendSlope,
                                    "analysis_timestamp", Instant.now().toString()
                            )
                    );
                }
        );

        processAttackResult(service, "slowloris", attackResults,
                (result) -> {
                    @SuppressWarnings("unchecked")
                    List<String> suspiciousPods = (List<String>) result.getOrDefault("suspicious_pods", Collections.emptyList());
                    int threshold = (int) result.getOrDefault("threshold", 0);
                    @SuppressWarnings("unchecked")
                    Map<String, Map<String, Object>> podStats = (Map<String, Map<String, Object>>)
                            result.getOrDefault("pod_statistics", Collections.emptyMap());

                    String severity = podStats.values().stream()
                            .anyMatch(stats -> (double) stats.getOrDefault("median", 0.0) > threshold * 1.5)
                            ? "critical" : "warning";

                    String podDetails = podStats.entrySet().stream()
                            .map(e -> String.format("%s (median: %.1f, autocorr: %.2f)",
                                    e.getKey(),
                                    (double) e.getValue().getOrDefault("median", 0.0),
                                    (double) e.getValue().getOrDefault("autocorrelation", 0.0)))
                            .collect(Collectors.joining(", "));

                    return new AlertMessage(
                            String.format(
                                    "SlowLoris alert on %s. Suspicious pods: %s. Details: %s",
                                    service,
                                    suspiciousPods.isEmpty() ? "none" : String.join(", ", suspiciousPods),
                                    podDetails
                            ),
                            severity,
                            Map.of(
                                    "suspicious_pods", suspiciousPods,
                                    "threshold", threshold,
                                    "pod_statistics", podStats,
                                    "analysis_timestamp", Instant.now().toString()
                            )
                    );
                }
        );

        processAttackResult(service, "dns_amplification", attackResults,
                (result) -> {
                    @SuppressWarnings("unchecked")
                    List<String> suspiciousPods = (List<String>) result.getOrDefault("suspicious_pods", Collections.emptyList());
                    int threshold = (int) result.getOrDefault("threshold", 0);
                    @SuppressWarnings("unchecked")
                    Map<String, Map<String, Object>> podStats = (Map<String, Map<String, Object>>)
                            result.getOrDefault("pod_statistics", Collections.emptyMap());

                    String severity = podStats.values().stream()
                            .anyMatch(stats -> (double) stats.getOrDefault("burstiness", 0.0) > 0.9)
                            ? "critical" : "warning";

                    String podDetails = podStats.entrySet().stream()
                            .map(e -> String.format("%s (median: %.1f, burstiness: %.2f)",
                                    e.getKey(),
                                    (double) e.getValue().getOrDefault("median", 0.0),
                                    (double) e.getValue().getOrDefault("burstiness", 0.0)))
                            .collect(Collectors.joining(", "));

                    return new AlertMessage(
                            String.format(
                                    "DNS Amplification alert on %s. Suspicious pods: %s. Details: %s",
                                    service,
                                    suspiciousPods.isEmpty() ? "none" : String.join(", ", suspiciousPods),
                                    podDetails
                            ),
                            severity,
                            Map.of(
                                    "suspicious_pods", suspiciousPods,
                                    "threshold", threshold,
                                    "pod_statistics", podStats,
                                    "analysis_timestamp", Instant.now().toString()
                            )
                    );
                }
        );

        if (attackResults.containsKey("statistical_analysis")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> statsAnalysis = (Map<String, Object>) attackResults.get("statistical_analysis");
            statsAnalysis.forEach((metric, analysis) -> {
                logger.debug("Statistical analysis for {} on service {}: {}", metric, service, analysis);
            });
        }
    }

    private void processAttackResult(String service, String attackType,
                                     Map<String, Object> attackResults,
                                     AttackMessageFormatter formatter) {
        try {
            if (!attackResults.containsKey(attackType)) {
                logger.warn("No {} results found for service {}", attackType, service);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> result = (Map<String, Object>) attackResults.get(attackType);

            if (result != null && Boolean.TRUE.equals(result.get("detected"))) {

                if(redisService.exists("attackMonitoringCooldown")) return;

                AlertMessage alert = formatter.format(result);
                redisService.commentProblem(
                        service,
                        alert.message() + "\n " + alert.metadata(),
                        alert.severity()
                );

                logger.warn("{} attack detected on service {}: {}",
                        attackType, service, alert.message());

                redisService.setWithExpiry("attackMonitoringCooldown", "frozen3", 10L);

            } else {
                logger.debug("No {} attack detected on service {}", attackType, service);
            }
        } catch (Exception e) {
            logger.error("Error processing {} results for service {}", attackType, service, e);
        }
    }

    private String generateDiagnosisMessage(MemoryAnalyzer.MemoryAnalysisResult result) {
        if (result.possibleLeak && result.possibleBloat) {
            return "Critical: Both memory leak and bloat detected. This indicates both sustained memory growth and erratic allocation patterns.";
        } else if (result.possibleLeak) {
            return "Memory leak detected. The service shows progressive, irreversible memory accumulation.";
        } else {
            return "Memory bloat detected. The service shows temporary excessive memory spikes but returns to baseline.";
        }
    }

    private void processMetric(String service, String metric) {
        String[] parts = metric.split("\\.");
        String measurement = parts[0];
        String field = parts.length > 1 ? parts[1] : "value";
        int windowMinutes = configProcessing.getMetricWindowMinutes(metric);

        InfluxMetricsProcessing.handleMetric(measurement, field, service, windowMinutes);
    }

    private void cleanupEmptyHosts(String service, String metric) {
        String[] parts = metric.split("\\.");
        String measurement = parts[0];

        MetricProcessing.cleanupInactiveHosts(measurement, service, 30);
    }

    private void sendScalingNotification(String service, int currentReplicas, int newReplicas,
                                         double avgLoad, double maxLoad, double p95Load) {
        String message = String.format(
                "**Service Scaling Notification**\n" +
                        "Service: %s\n" +
                        "Replicas: %d → %d\n" +
                        "Load Statistics:\n" +
                        "  - Average: %.1f%%\n" +
                        "  - Maximum: %.1f%%\n" +
                        "  - 95th Percentile: %.1f%%\n" +
                        "Action: %s",
                service,
                currentReplicas,
                newReplicas,
                avgLoad,
                maxLoad,
                p95Load,
                newReplicas > currentReplicas ? "SCALED UP" : "SCALED DOWN"
        );

        redisService.commentProblem(service, message, "scaling");
    }

    private double calculatePercentile(List<Double> values, double percentile) {
        if (values.isEmpty()) return 0;

        double[] sorted = values.stream()
                .mapToDouble(d -> d)
                .sorted()
                .toArray();

        double index = percentile * (sorted.length - 1);
        int lower = (int) Math.floor(index);
        int upper = (int) Math.ceil(index);

        if (lower == upper) {
            return sorted[lower];
        }

        return sorted[lower] * (1 - (index - lower)) + sorted[upper] * (index - lower);
    }
}

@FunctionalInterface
interface AttackMessageFormatter {
    AlertMessage format(Map<String, Object> result);
}

record AlertMessage(String message, String severity, Map<String, Object> metadata) {}