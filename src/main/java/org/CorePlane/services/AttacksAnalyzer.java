package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AttacksAnalyzer {
    private static final int SHORT_WINDOW_MINUTES = 5;
    private static final int LONG_WINDOW_MINUTES = 60;
    private static final int MIN_DATA_POINTS_FOR_ANALYSIS = 10;

    private final MetricsProcessing metricsProcessing;
    private final Map<String, String> detectionFields;
    private final Map<String, Integer> detectionThresholds;

    public AttacksAnalyzer(MetricsProcessing metricsProcessing, ConfigProcessing configProcessing) {
        this.metricsProcessing = metricsProcessing;
        this.detectionFields = configProcessing.getAttackDetectionFields();
        this.detectionThresholds = configProcessing.getAttackDetectionThresholds();
    }

    public Map<String, Object> analyzeForAttacks(String serviceName) {
        Map<String, Object> results = new HashMap<>();
        try {
            results.put("ddos_volumetric", detectVolumetricDDoS(serviceName));
            results.put("slowloris", detectSlowLoris(serviceName));
            results.put("dns_amplification", detectDnsAmplification(serviceName));
            results.put("statistical_analysis", performStatisticalAnalysis(serviceName));
        } catch (Exception e) {
            results.put("error", "Analysis failed: " + e.getMessage());
        }
        return results;
    }

    private Map<String, Object> detectVolumetricDDoS(String serviceName) {
        Map<String, Object> result = new HashMap<>();
        String fieldName = detectionFields.get("request_rate");

        List<Double> shortWindowRates = metricsProcessing.getMetricsInTimeWindow(
                "request_rate", fieldName, serviceName, SHORT_WINDOW_MINUTES);
        List<Double> longWindowRates = metricsProcessing.getMetricsInTimeWindow(
                "request_rate", fieldName, serviceName, LONG_WINDOW_MINUTES);

        if (shortWindowRates.size() < MIN_DATA_POINTS_FOR_ANALYSIS ||
                longWindowRates.size() < MIN_DATA_POINTS_FOR_ANALYSIS) {
            result.put("detected", false);
            result.put("reason", "Insufficient data points");
            return result;
        }

        double shortWindowMedian = calculateMedian(shortWindowRates);
        double longWindowMedian = calculateMedian(longWindowRates);
        double mad = calculateMAD(longWindowRates, longWindowMedian);
        double threshold = longWindowMedian + (3.5 * mad);
        double exceedanceRatio = shortWindowRates.stream()
                .filter(rate -> rate > threshold)
                .count() / (double) shortWindowRates.size();
        double trendSlope = calculateTrendSlope(shortWindowRates);

        result.put("detected", exceedanceRatio > 0.7 || trendSlope > 0.5);
        result.put("current_median_rate", shortWindowMedian);
        result.put("baseline_median_rate", longWindowMedian);
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
        List<Double> rates = metricsProcessing.getMetricsInTimeWindow(
                "request_rate", rateField, serviceName, LONG_WINDOW_MINUTES);

        if (rates.size() >= MIN_DATA_POINTS_FOR_ANALYSIS) {
            analysisResults.put("request_rate_analysis", analyzeTimeSeries(rates));
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

    // All mathematical calculation methods remain exactly the same
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
}