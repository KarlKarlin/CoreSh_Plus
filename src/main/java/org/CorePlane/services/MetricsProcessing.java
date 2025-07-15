package org.CorePlane.services;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
public class MetricsProcessing {
    private static final Logger logger = LoggerFactory.getLogger(MetricsProcessing.class);

    @Value("${influx.url}")
    private String influxUrl;

    @Value("${influx.username}")
    private String username;

    @Value("${influx.password}")
    private String password;

    @Value("${influx.db}")
    private String db;

    private final InfluxDB influxDB;
    private final Map<String, KalmanFilter> kalmanFilters = new ConcurrentHashMap<>();

    public MetricsProcessing(@Value("${influx.url}") String influxUrl,
                             @Value("${influx.username}") String username,
                             @Value("${influx.password}") String password) {
        this.influxDB = InfluxDBFactory.connect(influxUrl, username, password);
    }

    @Bean(destroyMethod = "close")
    public InfluxDB influxDB() {
        InfluxDB influxDB = InfluxDBFactory.connect(influxUrl, username, password);
        influxDB.setDatabase(db);
        influxDB.enableBatch(100, 200, TimeUnit.MILLISECONDS);
        return influxDB;
    }

    public void cleanupInactiveHosts(String measurement,
                                     String serviceName,
                                     int minutesThreshold) {
        try {
            Map<String, Instant> hostLastSeen = getHostLastSeenTimes(measurement, serviceName);
            Instant cutoff = Instant.now().minusSeconds(minutesThreshold * 60L);

            hostLastSeen.entrySet().stream()
                    .filter(entry -> entry.getValue().isBefore(cutoff))
                    .forEach(entry -> {
                        String host = entry.getKey();
                        try {
                            String dropQuery = String.format(
                                    "DROP SERIES WHERE \"host\" = '%s' AND \"service\" = '%s'",
                                    host, serviceName);
                            influxDB.query(new Query(dropQuery, db));
                            logger.info("Purged inactive host: {}", host);
                        } catch (Exception e) {
                            logger.error("Failed to purge host {}: {}", host, e.getMessage());
                        }
                    });
        } catch (Exception e) {
            logger.error("Host cleanup failed: {}", e.getMessage());
            throw new RuntimeException("Host cleanup failed", e);
        }
    }

    private Map<String, Instant> getHostLastSeenTimes(String measurement, String serviceName) {
        String query = String.format(
                "SELECT last(*) FROM \"%s\" WHERE \"service\" = '%s' GROUP BY \"host\"",
                measurement, serviceName);

        QueryResult result = influxDB.query(new Query(query, db));
        Map<String, Instant> lastSeen = new HashMap<>();

        result.getResults().forEach(r -> {
            if (r.getSeries() != null) {
                r.getSeries().forEach(s -> {
                    String host = s.getTags().get("host");
                    s.getValues().forEach(v -> {
                        Instant time = Instant.parse(v.get(0).toString());
                        lastSeen.put(host, time);
                    });
                });
            }
        });
        return lastSeen;
    }

    public List<Double> getMetricsInTimeWindow(String measurement,
                                               String field,
                                               String serviceName,
                                               int minutes) {
        try {
            long windowMs = minutes * 60 * 1000L;
            long timestampTolerance = 5000;

            String query = String.format(
                    "SELECT \"%s\" FROM \"%s\" WHERE \"service\" = '%s' " +
                            "AND time > now() - %dms GROUP BY \"host\" ORDER BY time ASC",
                    field, measurement, serviceName, windowMs);

            QueryResult result = influxDB.query(new Query(query, db));

            List<MetricPoint> allPoints = new ArrayList<>();
            for (QueryResult.Result queryResult : result.getResults()) {
                if (queryResult.getSeries() != null) {
                    for (QueryResult.Series series : queryResult.getSeries()) {
                        for (List<Object> values : series.getValues()) {
                            Instant time = Instant.parse(values.get(0).toString());
                            double value = ((Number) values.get(1)).doubleValue();
                            allPoints.add(new MetricPoint(time, value));
                        }
                    }
                }
            }

            allPoints.sort(Comparator.comparing(MetricPoint::getTime));

            List<Double> summedValues = new ArrayList<>();
            if (!allPoints.isEmpty()) {
                Instant currentBucketTime = allPoints.get(0).getTime();
                double currentSum = 0;
                int pointsInBucket = 0;

                for (MetricPoint point : allPoints) {
                    if (Math.abs(point.getTime().toEpochMilli() - currentBucketTime.toEpochMilli()) <= timestampTolerance) {
                        currentSum += point.getValue();
                        pointsInBucket++;
                    } else {
                        if (pointsInBucket > 0) {
                            summedValues.add(currentSum);
                        }
                        currentBucketTime = point.getTime();
                        currentSum = point.getValue();
                        pointsInBucket = 1;
                    }
                }
                if (pointsInBucket > 0) {
                    summedValues.add(currentSum);
                }
            }

            return summedValues;

        } catch (Exception e) {
            logger.error("Error getting metrics for {}:{} in service {}: {}",
                    measurement, field, serviceName, e.getMessage());
            return Collections.emptyList();
        }
    }

    public Map<String, Map<Instant, Double>> getMetricsByPod(String measurement,
                                                             String field,
                                                             String serviceName,
                                                             int minutes) {
        long windowMs = minutes * 60 * 1000L;
        String query = String.format(
                "SELECT \"%s\" FROM \"%s\" WHERE \"service\" = '%s' " +
                        "AND time > now() - %dms GROUP BY \"host\" ORDER BY time ASC",
                field, measurement, serviceName, windowMs);

        QueryResult result = influxDB.query(new Query(query, db));
        return extractGroupedMetricsFromResult(result);
    }

    public int predictRequiredPods(String measurement,
                                   String field,
                                   String serviceName,
                                   int currentPods,
                                   double podCapacity,
                                   int minPods,
                                   int maxPods,
                                   int predictionWindowMinutes) {
        try {
            validateInputParameters(podCapacity, minPods, maxPods, serviceName);

            Map<String, Map<Instant, Double>> podMetrics = getMetricsByPod(
                    measurement, field, serviceName, predictionWindowMinutes);

            if (podMetrics.isEmpty()) {
                logger.debug("No metrics available for service {}", serviceName);
                return currentPods;
            }

            updateKalmanFilters(podMetrics);
            LoadPrediction prediction = calculateLoadPredictions(podMetrics);
            int requiredPods = calculateRecommendedPods(prediction, podCapacity, minPods, maxPods);

            return determineFinalPodCount(requiredPods, currentPods, minPods, maxPods, serviceName);

        } catch (Exception e) {
            logger.error("Prediction failed for service {}: {}", serviceName, e.getMessage());
            return currentPods;
        }
    }

    private void validateInputParameters(double podCapacity,
                                         int minPods,
                                         int maxPods,
                                         String serviceName) {
        if (podCapacity <= 0 || minPods <= 0 || maxPods < minPods) {
            String msg = String.format("Invalid parameters for service %s: podCapacity=%.2f, min=%d, max=%d",
                    serviceName, podCapacity, minPods, maxPods);
            logger.warn(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    private void updateKalmanFilters(Map<String, Map<Instant, Double>> podMetrics) {
        podMetrics.forEach((pod, metrics) -> {
            kalmanFilters.computeIfAbsent(pod, k ->
                    new KalmanFilter(0.1, 0.5, 0.15));

            metrics.values().stream()
                    .max(Double::compare)
                    .ifPresent(latestValue -> kalmanFilters.get(pod).update(latestValue));
        });
    }

    private LoadPrediction calculateLoadPredictions(Map<String, Map<Instant, Double>> podMetrics) {
        double currentTotal = 0;
        double predictedTotal = 0;
        int count = 0;
        double maxPredicted = 0;

        for (Map.Entry<String, Map<Instant, Double>> entry : podMetrics.entrySet()) {
            KalmanFilter filter = kalmanFilters.get(entry.getKey());
            if (filter != null) {
                double currentLoad = entry.getValue().values().stream()
                        .mapToDouble(Double::doubleValue)
                        .average()
                        .orElse(0);

                currentTotal += currentLoad;
                double predicted = filter.predictNext();
                predictedTotal += predicted;
                maxPredicted = Math.max(maxPredicted, predicted);
                count++;
            }
        }

        if (count == 0) {
            return new LoadPrediction(0, 0, 0);
        }

        return new LoadPrediction(
                currentTotal / count,
                predictedTotal / count,
                maxPredicted
        );
    }

    private int calculateRecommendedPods(LoadPrediction prediction,
                                         double podCapacity,
                                         int minPods,
                                         int maxPods) {
        double targetUtilization = 0.85;
        double weightedLoad = (prediction.avgCurrentLoad * 0.4) + (prediction.avgPredictedLoad * 0.6);
        double effectiveLoad = Math.max(weightedLoad, prediction.peakPredictedLoad * 0.8);
        return Math.max(minPods, Math.min(maxPods, (int) Math.ceil(effectiveLoad / (podCapacity * targetUtilization))));
    }

    private int determineFinalPodCount(int recommendedPods,
                                       int currentPods,
                                       int minPods,
                                       int maxPods,
                                       String serviceName) {
        int boundedPods = Math.max(minPods, Math.min(maxPods, recommendedPods));
        int minStep = getDynamicScaleStep(currentPods);

        if (boundedPods > currentPods && (boundedPods - currentPods) >= minStep) {
            logger.info("Scaling recommended for {}: current={}, recommended={} (minStep={})",
                    serviceName, currentPods, boundedPods, minStep);
            return boundedPods;
        }

        logger.debug("No scaling needed for {}: current pods sufficient (current={}, recommended={})",
                serviceName, currentPods, boundedPods);
        return currentPods;
    }

    private int getDynamicScaleStep(int currentPods) {
        if (currentPods <= 3) return 1;
        if (currentPods <= 10) return 2;
        return (int) Math.max(2, Math.ceil(currentPods * 0.2));
    }

    private Map<String, Map<Instant, Double>> extractGroupedMetricsFromResult(QueryResult result) {
        Map<String, Map<Instant, Double>> podMetrics = new HashMap<>();
        result.getResults().stream()
                .filter(r -> r.getSeries() != null)
                .forEach(r -> r.getSeries().forEach(s -> {
                    String host = s.getTags().get("host");
                    Map<Instant, Double> timeValueMap = new LinkedHashMap<>();
                    s.getValues().stream()
                            .filter(v -> v.size() > 1)
                            .forEach(v -> {
                                Instant time = Instant.parse(v.get(0).toString());
                                Double value = ((Number) v.get(1)).doubleValue();
                                timeValueMap.put(time, value);
                            });
                    podMetrics.put(host, timeValueMap);
                }));
        return podMetrics;
    }

    private static class MetricPoint {
        private final Instant time;
        private final double value;

        public MetricPoint(Instant time, double value) {
            this.time = time;
            this.value = value;
        }

        public Instant getTime() {
            return time;
        }

        public double getValue() {
            return value;
        }
    }

    private static class LoadPrediction {
        final double avgCurrentLoad;
        final double avgPredictedLoad;
        final double peakPredictedLoad;

        LoadPrediction(double avgCurrentLoad, double avgPredictedLoad, double peakPredictedLoad) {
            this.avgCurrentLoad = avgCurrentLoad;
            this.avgPredictedLoad = avgPredictedLoad;
            this.peakPredictedLoad = peakPredictedLoad;
        }
    }
}