package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AttacksAnalyzer {
    private static final Logger logger = LoggerFactory.getLogger(AttacksAnalyzer.class);

    private static final int SHORT_WINDOW_MINUTES = 5;
    private static final int LONG_WINDOW_MINUTES = 60;
    private static final int MIN_DATA_POINTS_FOR_ANALYSIS = 10;

    private static final double SMOOTHING_FACTOR = 0.01;
    private static final double HISTORICAL_WEIGHT = 0.6;
    private static final double DEFAULT_WEIGHT = 0.4;
    private static final double MIN_PROBABILITY_THRESHOLD = 0.01;

    private static final String STATE_NORMAL = "NORMAL";
    private static final String STATE_DDOS = "DDoS_VOLUMETRIC";
    private static final String STATE_SLOWLORIS = "SLOWLORIS";
    private static final String STATE_DNS_AMP = "DNS_AMPLIFICATION";

    private final MetricsProcessing metricsProcessing;
    private final RedisService redisService;
    private final Map<String, String> detectionFields;
    private final Map<String, Integer> detectionThresholds;
    private final Map<String, Map<String, Double>> markovChainStates;

    public AttacksAnalyzer(MetricsProcessing metricsProcessing,
                           ConfigProcessing configProcessing,
                           RedisService redisService) {
        this.metricsProcessing = metricsProcessing;
        this.redisService = redisService;
        this.detectionFields = configProcessing.getAttackDetectionFields();
        this.detectionThresholds = configProcessing.getAttackDetectionThresholds();
        this.markovChainStates = initializeMarkovStates();
    }

    private Map<String, Map<String, Double>> initializeMarkovStates() {
        Map<String, Map<String, Double>> states = new HashMap<>();

        states.put(STATE_NORMAL, Map.of(
                STATE_NORMAL, 0.9,
                STATE_DDOS, 0.06,
                STATE_SLOWLORIS, 0.02,
                STATE_DNS_AMP, 0.02
        ));

        states.put(STATE_DDOS, Map.of(
                STATE_NORMAL, 0.3,
                STATE_DDOS, 0.6,
                STATE_SLOWLORIS, 0.05,
                STATE_DNS_AMP, 0.05
        ));

        states.put(STATE_SLOWLORIS, Map.of(
                STATE_NORMAL, 0.3,
                STATE_DDOS, 0.05,
                STATE_SLOWLORIS, 0.6,
                STATE_DNS_AMP, 0.05
        ));

        states.put(STATE_DNS_AMP, Map.of(
                STATE_NORMAL, 0.3,
                STATE_DDOS, 0.05,
                STATE_SLOWLORIS, 0.05,
                STATE_DNS_AMP, 0.6
        ));

        return states;
    }

    public Map<String, Object> analyzeForAttacks(String serviceName) {
        Map<String, Object> results = new HashMap<>();
        try {
            Map<String, Object> ddosResult = detectVolumetricDDoS(serviceName);
            Map<String, Object> slowlorisResult = detectSlowLoris(serviceName);
            Map<String, Object> dnsResult = detectDnsAmplification(serviceName);

            results.put("ddos_volumetric", ddosResult);
            results.put("slowloris", slowlorisResult);
            results.put("dns_amplification", dnsResult);
            results.put("statistical_analysis", performStatisticalAnalysis(serviceName));

            updateMarkovState(serviceName, results);

        } catch (Exception e) {
            logger.error("Analysis failed for service {}: {}", serviceName, e.getMessage(), e);
            results.put("error", "Analysis failed: " + e.getMessage());
        }
        return results;
    }

    public Map<String, Object> predictAttackProbabilities(String serviceName) {
        Map<String, Object> results = new HashMap<>();
        String redisKey = serviceName + ":AttackAnalyzer";

        try {
            boolean lockAcquired = redisService.acquireLock(redisKey);
            if (!lockAcquired) {
                results.put("error", "Could not acquire lock for prediction");
                return results;
            }

            try {
                String lastState = redisService.getLastState(redisKey);
                if (lastState == null) {
                    Map<String, Double> initialProbs = Map.of(
                            STATE_NORMAL, 0.85,
                            STATE_DDOS, 0.08,
                            STATE_SLOWLORIS, 0.04,
                            STATE_DNS_AMP, 0.03
                    );

                    results.put("current_state", STATE_NORMAL);
                    results.put("next_state_probabilities", initialProbs);
                    results.put("most_likely_next_state", STATE_NORMAL);
                    return results;
                }

                Map<String, Integer> transitions = redisService.getTransitionProbabilities(redisKey, lastState);
                Map<String, Double> nextStateProbabilities = calculateNextStateProbabilities(lastState, transitions);

                results.put("current_state", lastState);
                results.put("next_state_probabilities", nextStateProbabilities);
                results.put("most_likely_next_state", getMostLikelyState(nextStateProbabilities));

            } finally {
                redisService.releaseLock(redisKey);
            }

        } catch (Exception e) {
            logger.error("Prediction failed for service {}: {}", serviceName, e.getMessage(), e);
            results.put("error", "Prediction failed: " + e.getMessage());
        }
        return results;
    }

    private Map<String, Double> calculateNextStateProbabilities(String currentState, Map<String, Integer> transitions) {
        Map<String, Double> probabilities = new HashMap<>();
        Map<String, Double> defaultProbs = markovChainStates.getOrDefault(currentState,
                Map.of(STATE_NORMAL, 1.0));

        if (transitions == null || transitions.isEmpty()) {
            return applySmoothing(new HashMap<>(defaultProbs));
        }

        int totalHistorical = transitions.values().stream().mapToInt(Integer::intValue).sum();

        defaultProbs.keySet().forEach(state -> {
            double historicalCount = transitions.getOrDefault(state, 0);
            double historicalWeight = calculateHistoricalWeight(historicalCount, totalHistorical);
            double defaultProb = defaultProbs.getOrDefault(state, MIN_PROBABILITY_THRESHOLD);

            double combinedProb = (HISTORICAL_WEIGHT * historicalWeight) +
                    (DEFAULT_WEIGHT * defaultProb);

            probabilities.put(state, combinedProb);
        });

        return normalizeProbabilities(applySmoothing(probabilities));
    }

    private double calculateHistoricalWeight(double count, double total) {
        if (total == 0) return 0;
        return Math.log1p(count) / Math.log1p(total);
    }

    private Map<String, Double> applySmoothing(Map<String, Double> probabilities) {
        probabilities.replaceAll((k, v) -> Math.max(v, MIN_PROBABILITY_THRESHOLD) + SMOOTHING_FACTOR);
        return probabilities;
    }

    private Map<String, Double> normalizeProbabilities(Map<String, Double> probabilities) {
        double sum = probabilities.values().stream().mapToDouble(Double::doubleValue).sum();

        if (sum <= 0) {
            double uniformProb = 1.0 / probabilities.size();
            probabilities.replaceAll((k, v) -> uniformProb);
            return probabilities;
        }

        probabilities.replaceAll((k, v) -> {
            double normalized = v / sum;
            return normalized < MIN_PROBABILITY_THRESHOLD ? MIN_PROBABILITY_THRESHOLD : normalized;
        });

        double newSum = probabilities.values().stream().mapToDouble(Double::doubleValue).sum();
        probabilities.replaceAll((k, v) -> v / newSum);

        return probabilities;
    }

    private void updateMarkovState(String serviceName, Map<String, Object> detectionResults) {
        String currentState = determineCurrentState(detectionResults);
        String redisKey = serviceName + ":AttackAnalyzer";

        try {
            boolean lockAcquired = redisService.acquireLock(redisKey);
            if (!lockAcquired) {
                logger.warn("Failed to acquire lock for state update: {}", serviceName);
                return;
            }

            String lastState = redisService.getLastState(redisKey);
            if (lastState == null) {
                lastState = STATE_NORMAL;
            }

            if (!currentState.equals(lastState)) {
                redisService.saveStateTransition(redisKey, lastState, currentState);
            }

        } catch (Exception e) {
            logger.error("Error updating state for service {}: {}", serviceName, e.getMessage(), e);
        } finally {
            redisService.releaseLock(redisKey);
        }
    }

    private String determineCurrentState(Map<String, Object> detectionResults) {
        try {
            boolean ddosDetected = (boolean) ((Map<?, ?>) detectionResults.get("ddos_volumetric")).get("detected");
            boolean slowlorisDetected = (boolean) ((Map<?, ?>) detectionResults.get("slowloris")).get("detected");
            boolean dnsDetected = (boolean) ((Map<?, ?>) detectionResults.get("dns_amplification")).get("detected");

            if (ddosDetected) return STATE_DDOS;
            if (slowlorisDetected) return STATE_SLOWLORIS;
            if (dnsDetected) return STATE_DNS_AMP;
        } catch (Exception e) {
            logger.error("Error determining current state", e);
        }
        return STATE_NORMAL;
    }

    private String getMostLikelyState(Map<String, Double> probabilities) {
        return probabilities.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(STATE_NORMAL);
    }

    private Map<String, Object> detectVolumetricDDoS(String serviceName) {
        Map<String, Object> result = new HashMap<>();
        String fieldName = detectionFields.get("request_rate");

        Map<String, Map<Instant, Double>> podMetrics = metricsProcessing.getMetricsByPod(
                "request_rate", fieldName, serviceName, LONG_WINDOW_MINUTES);

        if (podMetrics.isEmpty()) {
            result.put("detected", false);
            result.put("reason", "No data available");
            return result;
        }

        List<Double> allValues = podMetrics.values().stream()
                .flatMap(m -> m.values().stream())
                .collect(Collectors.toList());

        if (allValues.size() < MIN_DATA_POINTS_FOR_ANALYSIS) {
            result.put("detected", false);
            result.put("reason", "Insufficient data points");
            return result;
        }

        List<Double> recentValues = getRecentValues(podMetrics, SHORT_WINDOW_MINUTES);

        double baselineMedian = calculateMedian(allValues);
        double recentMedian = calculateMedian(recentValues);
        double mad = calculateMAD(allValues, baselineMedian);
        double threshold = baselineMedian + (3.5 * mad);

        double exceedanceRatio = recentValues.stream()
                .filter(rate -> rate > threshold)
                .count() / (double) recentValues.size();

        double trendSlope = calculateTrendSlope(recentValues);

        result.put("detected", exceedanceRatio > 0.7 || trendSlope > 0.5);
        result.put("current_median_rate", recentMedian);
        result.put("baseline_median_rate", baselineMedian);
        result.put("mad", mad);
        result.put("threshold", threshold);
        result.put("exceedance_ratio", exceedanceRatio);
        result.put("trend_slope", trendSlope);

        return result;
    }

    private Map<String, Object> detectSlowLoris(String serviceName) {
        Map<String, Object> result = new HashMap<>();
        String fieldName = detectionFields.get("open_connections");
        Map<String, Map<Instant, Double>> connectionsByPod = metricsProcessing.getMetricsByPod(
                "open_connections", fieldName, serviceName, SHORT_WINDOW_MINUTES);

        List<String> suspiciousPods = new ArrayList<>();
        Map<String, Map<String, Object>> podStatistics = new HashMap<>();

        connectionsByPod.forEach((pod, metrics) -> {
            List<Double> values = new ArrayList<>(metrics.values());
            if (values.size() < MIN_DATA_POINTS_FOR_ANALYSIS) return;

            double median = calculateMedian(values);
            double mad = calculateMAD(values, median);
            double entropy = calculateEntropy(values);
            double autocorrelation = calculateAutocorrelation(values, 1);
            boolean isSuspicious = (autocorrelation > 0.8) && (entropy < 1.0) &&
                    (median > detectionThresholds.get("slowloris_connections"));

            Map<String, Object> stats = new HashMap<>();
            stats.put("median", median);
            stats.put("mad", mad);
            stats.put("entropy", entropy);
            stats.put("autocorrelation", autocorrelation);
            stats.put("suspicious", isSuspicious);
            podStatistics.put(pod, stats);

            if (isSuspicious) suspiciousPods.add(pod);
        });

        result.put("detected", !suspiciousPods.isEmpty());
        result.put("pod_statistics", podStatistics);
        result.put("suspicious_pods", suspiciousPods);
        result.put("threshold", detectionThresholds.get("slowloris_connections"));
        return result;
    }

    private Map<String, Object> detectDnsAmplification(String serviceName) {
        Map<String, Object> result = new HashMap<>();
        String fieldName = detectionFields.get("dns_queries");
        Map<String, Map<Instant, Double>> dnsMetrics = metricsProcessing.getMetricsByPod(
                "dns_queries", fieldName, serviceName, SHORT_WINDOW_MINUTES);

        List<String> suspiciousPods = new ArrayList<>();
        Map<String, Map<String, Object>> podStatistics = new HashMap<>();

        dnsMetrics.forEach((pod, metrics) -> {
            List<Double> values = new ArrayList<>(metrics.values());
            if (values.size() < MIN_DATA_POINTS_FOR_ANALYSIS) return;

            double median = calculateMedian(values);
            double mad = calculateMAD(values, median);
            double burstiness = calculateBurstiness(values);
            boolean hasChangePoint = detectChangePoint(values);
            boolean isSuspicious = (burstiness > 0.8) ||
                    (hasChangePoint && median > detectionThresholds.get("dns_query_spike"));

            Map<String, Object> stats = new HashMap<>();
            stats.put("median", median);
            stats.put("mad", mad);
            stats.put("burstiness", burstiness);
            stats.put("change_point_detected", hasChangePoint);
            stats.put("suspicious", isSuspicious);
            podStatistics.put(pod, stats);

            if (isSuspicious) suspiciousPods.add(pod);
        });

        result.put("detected", !suspiciousPods.isEmpty());
        result.put("pod_statistics", podStatistics);
        result.put("suspicious_pods", suspiciousPods);
        result.put("threshold", detectionThresholds.get("dns_query_spike"));
        return result;
    }

    private Map<String, Object> performStatisticalAnalysis(String serviceName) {
        Map<String, Object> analysisResults = new HashMap<>();
        String rateField = detectionFields.get("request_rate");

        Map<String, Map<Instant, Double>> rateData = metricsProcessing.getMetricsByPod(
                "request_rate", rateField, serviceName, LONG_WINDOW_MINUTES);

        if (!rateData.isEmpty()) {
            List<Double> allRates = rateData.values().stream()
                    .flatMap(m -> m.values().stream())
                    .collect(Collectors.toList());

            if (allRates.size() >= MIN_DATA_POINTS_FOR_ANALYSIS) {
                analysisResults.put("request_rate_analysis", analyzeTimeSeries(allRates));
            }
        }

        String connField = detectionFields.get("open_connections");
        Map<String, Map<Instant, Double>> connData = metricsProcessing.getMetricsByPod(
                "open_connections", connField, serviceName, LONG_WINDOW_MINUTES);

        Map<String, Object> connectionAnalysis = new HashMap<>();
        connData.forEach((pod, metrics) -> {
            List<Double> values = new ArrayList<>(metrics.values());
            if (values.size() >= MIN_DATA_POINTS_FOR_ANALYSIS) {
                connectionAnalysis.put(pod, analyzeTimeSeries(values));
            }
        });
        analysisResults.put("connection_analysis", connectionAnalysis);
        return analysisResults;
    }

    private Map<String, Object> analyzeTimeSeries(List<Double> values) {
        Map<String, Object> analysis = new HashMap<>();
        analysis.put("mean", calculateMean(values));
        analysis.put("median", calculateMedian(values));
        analysis.put("std_dev", calculateStdDev(values));
        analysis.put("mad", calculateMAD(values, calculateMedian(values)));
        analysis.put("iqr", calculateInterquartileRange(values));
        analysis.put("skewness", calculateSkewness(values));
        analysis.put("kurtosis", calculateKurtosis(values));
        analysis.put("trend_slope", calculateTrendSlope(values));
        analysis.put("autocorrelation_lag1", calculateAutocorrelation(values, 1));
        analysis.put("hurst_exponent", estimateHurstExponent(values));
        analysis.put("entropy", calculateEntropy(values));
        analysis.put("outliers", detectOutliersUsingMAD(values));
        analysis.put("change_points", detectChangePoints(values));
        return analysis;
    }

    public Map<String, Object> getComprehensiveAnalysis(String serviceName) {
        Map<String, Object> analysis = analyzeForAttacks(serviceName);
        Map<String, Object> prediction = predictAttackProbabilities(serviceName);

        Map<String, Object> result = new HashMap<>();
        result.put("current_analysis", analysis);
        result.put("prediction", prediction);
        result.put("risk_assessment", calculateCombinedRisk(analysis, prediction));

        return result;
    }

    private Map<String, Object> calculateCombinedRisk(Map<String, Object> analysis, Map<String, Object> prediction) {
        Map<String, Object> riskAssessment = new HashMap<>();
        double ddosRisk = 0, slowlorisRisk = 0, dnsRisk = 0;

        try {
            boolean ddosDetected = (boolean) ((Map<?, ?>) analysis.get("ddos_volumetric")).get("detected");
            boolean slowlorisDetected = (boolean) ((Map<?, ?>) analysis.get("slowloris")).get("detected");
            boolean dnsDetected = (boolean) ((Map<?, ?>) analysis.get("dns_amplification")).get("detected");

            @SuppressWarnings("unchecked")
            Map<String, Double> probabilities = (Map<String, Double>) prediction.get("next_state_probabilities");

            if (probabilities != null) {
                ddosRisk = probabilities.getOrDefault(STATE_DDOS, 0.0);
                slowlorisRisk = probabilities.getOrDefault(STATE_SLOWLORIS, 0.0);
                dnsRisk = probabilities.getOrDefault(STATE_DNS_AMP, 0.0);

                if (ddosDetected) ddosRisk *= 1.5;
                if (slowlorisDetected) slowlorisRisk *= 1.5;
                if (dnsDetected) dnsRisk *= 1.5;

                double maxRisk = Math.max(ddosRisk, Math.max(slowlorisRisk, dnsRisk));
                if (maxRisk > 0) {
                    ddosRisk = Math.min(1, ddosRisk / maxRisk);
                    slowlorisRisk = Math.min(1, slowlorisRisk / maxRisk);
                    dnsRisk = Math.min(1, dnsRisk / maxRisk);
                }

                riskAssessment.put("ddos_risk", ddosRisk);
                riskAssessment.put("slowloris_risk", slowlorisRisk);
                riskAssessment.put("dns_amplification_risk", dnsRisk);
                riskAssessment.put("overall_risk", (ddosRisk + slowlorisRisk + dnsRisk) / 3);
            }
        } catch (Exception e) {
            logger.error("Error calculating combined risk", e);
        }

        return riskAssessment;
    }

    private double calculateMedian(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int size = sorted.size();
        if (size % 2 == 0) {
            return (sorted.get(size/2 - 1) + sorted.get(size/2)) / 2.0;
        } else {
            return sorted.get(size/2);
        }
    }

    private double calculateInterquartileRange(List<Double> values) {
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        int size = sorted.size();
        double q1 = sorted.get(size/4);
        double q3 = sorted.get(3*size/4);
        return q3 - q1;
    }

    private double calculateMAD(List<Double> values, double median) {
        List<Double> absoluteDeviations = values.stream()
                .map(v -> Math.abs(v - median))
                .collect(Collectors.toList());
        return calculateMedian(absoluteDeviations);
    }

    private double calculateEntropy(List<Double> values) {
        double sum = values.stream().mapToDouble(Double::doubleValue).sum();
        if (sum == 0) return 0;
        return -values.stream()
                .map(v -> v/sum)
                .filter(p -> p > 0)
                .mapToDouble(p -> p * Math.log(p) / Math.log(2))
                .sum();
    }

    private double calculateAutocorrelation(List<Double> values, int lag) {
        if (lag >= values.size()) return 0;
        double mean = calculateMean(values);
        double variance = values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0);
        if (variance == 0) return 0;
        double autocovariance = 0;
        for (int i = 0; i < values.size() - lag; i++) {
            autocovariance += (values.get(i) - mean) * (values.get(i + lag) - mean);
        }
        return autocovariance / (values.size() - lag) / variance;
    }

    private double calculateTrendSlope(List<Double> values) {
        int n = values.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += values.get(i);
            sumXY += i * values.get(i);
            sumX2 += i * i;
        }
        double numerator = n * sumXY - sumX * sumY;
        double denominator = n * sumX2 - sumX * sumX;
        return denominator != 0 ? numerator / denominator : 0;
    }

    private double calculateBurstiness(List<Double> values) {
        double mean = calculateMean(values);
        double stdDev = calculateStdDev(values);
        return (stdDev - mean) / (stdDev + mean);
    }

    private boolean detectChangePoint(List<Double> values) {
        double mean = calculateMean(values);
        double stdDev = calculateStdDev(values);
        if (stdDev == 0) return false;
        double cusum = 0;
        double threshold = 5 * stdDev;
        for (double value : values) {
            cusum = Math.max(0, cusum + (value - mean - stdDev));
            if (cusum > threshold) return true;
        }
        return false;
    }

    private List<Integer> detectChangePoints(List<Double> values) {
        List<Integer> changePoints = new ArrayList<>();
        int n = values.size();
        if (n < 10) return changePoints;
        double[] cumulativeSum = new double[n + 1];
        for (int i = 1; i <= n; i++) {
            cumulativeSum[i] = cumulativeSum[i - 1] + values.get(i - 1);
        }
        for (int k = 5; k < n - 5; k++) {
            double s1 = cumulativeSum[k];
            double s2 = cumulativeSum[n] - cumulativeSum[k];
            double diff = Math.abs(s1/k - s2/(n - k));
            double pooledStd = Math.sqrt(
                    (calculateVariance(values.subList(0, k)) / k) +
                            (calculateVariance(values.subList(k, n)) / (n - k))
            );
            if (pooledStd > 0 && diff > 2.576 * pooledStd) {
                changePoints.add(k);
            }
        }
        return changePoints;
    }

    private List<Double> detectOutliersUsingMAD(List<Double> values) {
        double median = calculateMedian(values);
        double mad = calculateMAD(values, median);
        if (mad == 0) return Collections.emptyList();
        double threshold = median + 3.5 * mad;
        return values.stream()
                .filter(v -> v > threshold)
                .collect(Collectors.toList());
    }

    private double calculateSkewness(List<Double> values) {
        double mean = calculateMean(values);
        double stdDev = calculateStdDev(values);
        if (stdDev == 0) return 0;
        return values.stream()
                .mapToDouble(v -> Math.pow((v - mean)/stdDev, 3))
                .sum() / values.size();
    }

    private double calculateKurtosis(List<Double> values) {
        double mean = calculateMean(values);
        double stdDev = calculateStdDev(values);
        if (stdDev == 0) return 0;
        return (values.stream()
                .mapToDouble(v -> Math.pow((v - mean)/stdDev, 4))
                .sum() / values.size()) - 3;
    }

    private double estimateHurstExponent(List<Double> values) {
        int n = values.size();
        if (n < 10) return 0.5;
        double[] cumulativeDeviations = new double[n];
        double mean = calculateMean(values);
        cumulativeDeviations[0] = values.get(0) - mean;
        for (int i = 1; i < n; i++) {
            cumulativeDeviations[i] = cumulativeDeviations[i-1] + (values.get(i) - mean);
        }
        double range = max(cumulativeDeviations) - min(cumulativeDeviations);
        double stdDev = calculateStdDev(values);
        return stdDev > 0 ? Math.log(range / stdDev) / Math.log(n / 2.0) : 0.5;
    }

    private double max(double[] values) {
        double max = Double.MIN_VALUE;
        for (double v : values) if (v > max) max = v;
        return max;
    }

    private double min(double[] values) {
        double min = Double.MAX_VALUE;
        for (double v : values) if (v < min) min = v;
        return min;
    }

    private double calculateMean(List<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0);
    }

    private double calculateStdDev(List<Double> values) {
        double mean = calculateMean(values);
        return Math.sqrt(values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0));
    }

    private double calculateVariance(List<Double> values) {
        double mean = calculateMean(values);
        return values.stream()
                .mapToDouble(v -> Math.pow(v - mean, 2))
                .average().orElse(0);
    }

    private List<Double> getRecentValues(Map<String, Map<Instant, Double>> podMetrics, int minutes) {
        Instant cutoff = Instant.now().minusSeconds(minutes * 60L);
        return podMetrics.values().stream()
                .flatMap(m -> m.entrySet().stream()
                        .filter(e -> e.getKey().isAfter(cutoff))
                        .map(Map.Entry::getValue))
                .collect(Collectors.toList());
    }
}