package org.CorePlane.services;

import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class MemoryAnalyzer {

    private final MetricsProcessing metricsProcessing;

    public static final double LEAK_THRESHOLD = 1.5;
    public static final double BLOAT_THRESHOLD = 15.0;

    public MemoryAnalyzer(MetricsProcessing metricsProcessing) {
        this.metricsProcessing = metricsProcessing;
    }

    public MemoryAnalysisResult analyzeMemory(String serviceName, int minutes) {
        Map<Instant, Double> data = metricsProcessing.getMetricsWithTimestampsInWindow(
                "memory", "used_percent", serviceName, minutes);

        if (data.size() < 3) return new MemoryAnalysisResult(false, false, 0, 0);

        List<Instant> times = new ArrayList<>(data.keySet());
        List<Double> values = new ArrayList<>(data.values());

        LinearRegression baseline = calculateLinearBaseline(times, values);
        double actualArea = integrate(times, values);
        double linearArea = integrateLinear(times, baseline);
        double areaRatio = linearArea == 0 ? 0 : actualArea / linearArea;
        double stdDev = calculateStandardDeviation(values, baseline, times);

        boolean possibleLeak = areaRatio > LEAK_THRESHOLD;
        boolean possibleBloat = stdDev > BLOAT_THRESHOLD;

        return new MemoryAnalysisResult(possibleLeak, possibleBloat, areaRatio, stdDev);
    }

    private LinearRegression calculateLinearBaseline(List<Instant> times, List<Double> values) {
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
        int n = times.size();
        double firstTime = times.get(0).getEpochSecond();

        for (int i = 0; i < n; i++) {
            double x = times.get(i).getEpochSecond() - firstTime;
            double y = values.get(i);
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumXX += x * x;
        }

        double slope = (n * sumXY - sumX * sumY) / (n * sumXX - sumX * sumX);
        double intercept = (sumY - slope * sumX) / n;

        return new LinearRegression(slope, intercept, firstTime);
    }

    private double integrate(List<Instant> times, List<Double> values) {
        double sum = 0;
        for (int i = 1; i < times.size(); i++) {
            double dt = times.get(i).getEpochSecond() - times.get(i-1).getEpochSecond();
            double avg = (values.get(i) + values.get(i-1)) / 2;
            sum += avg * dt;
        }
        return sum;
    }

    private double integrateLinear(List<Instant> times, LinearRegression baseline) {
        double t1 = times.get(0).getEpochSecond() - baseline.referenceTime;
        double t2 = times.get(times.size()-1).getEpochSecond() - baseline.referenceTime;
        return 0.5 * baseline.slope * (t2*t2 - t1*t1) + baseline.intercept * (t2 - t1);
    }

    private double calculateStandardDeviation(List<Double> values, LinearRegression baseline, List<Instant> times) {

        if (values == null || times == null || values.isEmpty() || times.isEmpty() || values.size() != times.size()) {
            return 0;
        }

        double sum = 0;
        int n = values.size();
        double ref = baseline.referenceTime;

        for (int i = 0; i < n; i++) {
            double t = times.get(i).getEpochSecond() - ref;
            double expected = baseline.slope * t + baseline.intercept;
            double deviation = values.get(i) - expected;
            sum += deviation * deviation;
        }

        return Math.sqrt(sum / n);
    }

    static class LinearRegression {
        final double slope;
        final double intercept;
        final double referenceTime;

        LinearRegression(double slope, double intercept, double referenceTime) {
            this.slope = slope;
            this.intercept = intercept;
            this.referenceTime = referenceTime;
        }
    }

    static class MemoryAnalysisResult {
        final boolean possibleLeak;
        final boolean possibleBloat;
        final double areaRatio;
        final double standardDeviation;

        MemoryAnalysisResult(boolean possibleLeak, boolean possibleBloat,
                             double areaRatio, double standardDeviation) {
            this.possibleLeak = possibleLeak;
            this.possibleBloat = possibleBloat;
            this.areaRatio = areaRatio;
            this.standardDeviation = standardDeviation;
        }
    }
}
