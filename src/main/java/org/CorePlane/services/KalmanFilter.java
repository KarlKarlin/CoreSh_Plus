package org.CorePlane.services;

public class KalmanFilter {
    private double processNoise;
    private double measurementNoise;
    private double estimatedError;
    private double currentEstimate;
    private double kalmanGain;
    private double lastDelta;
    private double trendFactor;

    public KalmanFilter(double processNoise, double measurementNoise) {
        this.processNoise = processNoise;
        this.measurementNoise = measurementNoise;
        this.estimatedError = 1.0;
        this.currentEstimate = 0.0;
        this.trendFactor = 0.3;
    }

    public double update(double measurement) {
        double delta = measurement - currentEstimate;
        estimatedError += processNoise;

        kalmanGain = estimatedError / (estimatedError + measurementNoise);
        currentEstimate += kalmanGain * delta;
        estimatedError = (1 - kalmanGain) * estimatedError;

        lastDelta = delta * kalmanGain;
        return currentEstimate;
    }

    public double predictNext() {
        return currentEstimate + (lastDelta * trendFactor);
    }

    public void reset() {
        this.estimatedError = 1.0;
        this.currentEstimate = 0.0;
        this.lastDelta = 0.0;
    }
}