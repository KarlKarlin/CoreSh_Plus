package org.CorePlane.services;

public class KalmanFilter {
    private final double processNoise;
    private final double measurementNoise;
    private double errorCovariance;
    private double currentEstimate;
    private double previousEstimate;
    private double kalmanGain;
    private boolean initialized;
    private final double stabilityFactor;

    public KalmanFilter(double processNoise, double measurementNoise, double stabilityFactor) {
        if (processNoise <= 0 || measurementNoise <= 0) {
            throw new IllegalArgumentException("Noise values must be positive");
        }
        this.processNoise = processNoise;
        this.measurementNoise = measurementNoise;
        this.errorCovariance = 1.0;
        this.currentEstimate = 0.0;
        this.initialized = false;
        this.stabilityFactor = Math.max(0.01, Math.min(0.5, stabilityFactor));
    }

    public void update(double measurement) {
        if (!initialized) {
            currentEstimate = measurement;
            errorCovariance = measurementNoise;
            initialized = true;
            return;
        }

        errorCovariance += processNoise;

        kalmanGain = errorCovariance / (errorCovariance + measurementNoise);
        currentEstimate += kalmanGain * (measurement - currentEstimate);
        errorCovariance = (1 - kalmanGain) * errorCovariance;

        previousEstimate = currentEstimate;
    }


    public double predictNext() {
        if (!initialized) {
            throw new IllegalStateException("Filter not initialized with measurements");
        }
        double trend = (currentEstimate - previousEstimate) * stabilityFactor;
        return currentEstimate + trend;
    }
}