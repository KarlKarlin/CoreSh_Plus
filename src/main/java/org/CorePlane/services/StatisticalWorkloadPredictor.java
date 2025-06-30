package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import java.util.*;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.*;

public class StatisticalWorkloadPredictor implements WorkloadPredictor {

    private final RedisService redisService;
    private final ConfigProcessing configProcessing;
    private final DockerSwarmService dockerSwarmService;
    public final int analysisWindowSize;
    private final double smoothingFactor;
    private final double predictionPercentile;
    private final double scalingBufferFactor;
    private final double emergencyThreshold;

    public StatisticalWorkloadPredictor(RedisService redisService,
                                        ConfigProcessing configProcessing,
                                        DockerSwarmService dockerSwarmService) {
        this(redisService, configProcessing, dockerSwarmService,
                24, 0.25, 0.97, 0.25, 0.9);
    }

    public StatisticalWorkloadPredictor(RedisService redisService,
                                        ConfigProcessing configProcessing,
                                        DockerSwarmService dockerSwarmService,
                                        int analysisWindowSize,
                                        double smoothingFactor,
                                        double predictionPercentile,
                                        double scalingBufferFactor,
                                        double emergencyThreshold) {
        this.redisService = redisService;
        this.configProcessing = configProcessing;
        this.dockerSwarmService = dockerSwarmService;
        this.analysisWindowSize = analysisWindowSize;
        this.smoothingFactor = Math.min(Math.max(smoothingFactor, 0.1), 0.5);
        this.predictionPercentile = Math.min(Math.max(predictionPercentile, 0.9), 0.99);
        this.scalingBufferFactor = Math.min(Math.max(scalingBufferFactor, 0.15), 0.35);
        this.emergencyThreshold = Math.min(Math.max(emergencyThreshold, 0.8), 0.95);
    }

    @Override
    public double predictRequiredInstances(String serviceName) {
        int currentReplicas = dockerSwarmService.getCurrentReplicasForService(serviceName);
        double WUM = configProcessing.unitsPerPod();

        List<Double> hourlyLoads = redisService.getLtForDay(serviceName);
        if (hourlyLoads.isEmpty()) {
            return configProcessing.getMinReplicasForService(serviceName);
        }

        double currentUtilization = hourlyLoads.get(hourlyLoads.size() - 1);
        double peakWorkload = Collections.max(hourlyLoads);
        double currentCapacity = currentReplicas * WUM;

        if (currentUtilization > currentCapacity * emergencyThreshold) {
            double emergencyReplicas = Math.ceil(peakWorkload / WUM) + 1;
            return applyReplicaConstraints(serviceName, emergencyReplicas, currentReplicas);
        }

        List<Double> historicalWorkloads = redisService.getAllWLForService(serviceName);

        List<Double> smoothedWorkloads = exponentialSmoothing(hourlyLoads, smoothingFactor);
        double[] workloadArray = smoothedWorkloads.stream().mapToDouble(Double::doubleValue).toArray();

        double[] frequencyComponents = performFFT(workloadArray);
        double weightedPrediction = weightedMovingAveragePrediction(workloadArray);
        double percentilePrediction = percentileBasedPrediction(workloadArray, predictionPercentile);
        double fftAdjustedPrediction = fftAdjustedPrediction(workloadArray, frequencyComponents);

        double combinedPrediction = Math.max(
                Math.max(weightedPrediction, percentilePrediction),
                fftAdjustedPrediction
        );

        combinedPrediction = Math.max(combinedPrediction, peakWorkload * 0.95);

        if (!historicalWorkloads.isEmpty() && historicalWorkloads.size() > 24) {
            double avgGrowth = calculateAggressiveGrowth(historicalWorkloads);
            combinedPrediction *= (1 + Math.max(avgGrowth, 0));
        }

        double requiredReplicas = Math.ceil(combinedPrediction / WUM);

        if (currentUtilization > currentCapacity * 0.7) {
            requiredReplicas = Math.ceil(requiredReplicas * (1 + scalingBufferFactor));
        }

        return applyReplicaConstraints(serviceName, requiredReplicas, currentReplicas);
    }

    private double calculateAggressiveGrowth(List<Double> values) {
        if (values.size() < 48) return 0;

        List<Double> dailyPeaks = new ArrayList<>();
        for (int i = 0; i < values.size() / 24; i++) {
            int start = i * 24;
            int end = Math.min(start + 24, values.size());
            dailyPeaks.add(Collections.max(values.subList(start, end)));
        }

        if (dailyPeaks.size() < 2) return 0;

        double totalGrowth = 0;
        int count = 0;
        for (int i = 1; i < dailyPeaks.size(); i++) {
            if (dailyPeaks.get(i-1) > 0) {
                totalGrowth += (dailyPeaks.get(i) - dailyPeaks.get(i-1)) / dailyPeaks.get(i-1);
                count++;
            }
        }

        return count > 0 ? totalGrowth / count : 0;
    }

    private double applyReplicaConstraints(String serviceName, double requiredReplicas, int currentReplicas) {
        int minReplicas = configProcessing.getMinReplicasForService(serviceName);
        int maxReplicas = configProcessing.getMaxReplicasForService(serviceName);

        double minChange = Math.max(1, currentReplicas * 0.2);
        if (Math.abs(requiredReplicas - currentReplicas) < minChange) {
            return currentReplicas;
        }

        return Math.max(minReplicas, Math.min(Math.ceil(requiredReplicas), maxReplicas));
    }

    private List<Double> exponentialSmoothing(List<Double> data, double alpha) {
        List<Double> smoothed = new ArrayList<>();
        if (data.isEmpty()) return smoothed;

        smoothed.add(data.get(0));
        for (int i = 1; i < data.size(); i++) {
            smoothed.add(alpha * data.get(i) + (1 - alpha) * smoothed.get(i-1));
        }
        return smoothed;
    }

    private double[] performFFT(double[] data) {
        int newSize = nextPowerOfTwo(data.length);
        double[] paddedData = Arrays.copyOf(data, newSize);

        FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
        Complex[] complexResult = fft.transform(paddedData, TransformType.FORWARD);

        double[] magnitudes = new double[complexResult.length];
        for (int i = 0; i < complexResult.length; i++) {
            magnitudes[i] = complexResult[i].abs();
        }
        return magnitudes;
    }

    private int nextPowerOfTwo(int n) {
        return (int) Math.pow(2, Math.ceil(Math.log(n) / Math.log(2)));
    }

    private double weightedMovingAveragePrediction(double[] data) {
        double weightedSum = 0;
        double weightSum = 0;

        for (int i = 0; i < data.length; i++) {
            double weight = Math.pow(0.92, data.length - i - 1);
            weightedSum += data[i] * weight;
            weightSum += weight;
        }

        return weightedSum / weightSum;
    }

    private double percentileBasedPrediction(double[] data, double percentile) {
        double[] sortedData = Arrays.copyOf(data, data.length);
        Arrays.sort(sortedData);

        double index = percentile * (sortedData.length - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);

        if (lowerIndex == upperIndex) {
            return sortedData[lowerIndex];
        }

        double weight = index - lowerIndex;
        return sortedData[lowerIndex] * (1 - weight) + sortedData[upperIndex] * weight;
    }

    private double fftAdjustedPrediction(double[] data, double[] frequencyComponents) {
        double[] sortedMagnitudes = Arrays.copyOf(frequencyComponents, frequencyComponents.length);
        Arrays.sort(sortedMagnitudes);
        double threshold = sortedMagnitudes[(int) (0.85 * sortedMagnitudes.length)];

        int significantFrequencies = 0;
        for (double mag : frequencyComponents) {
            if (mag > threshold) {
                significantFrequencies++;
            }
        }

        double basePrediction = weightedMovingAveragePrediction(data);
        double frequencyFactor = 1 + (0.12 * significantFrequencies);
        return Math.min(basePrediction * frequencyFactor, basePrediction * 1.5);
    }
}