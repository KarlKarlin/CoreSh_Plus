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
        long windowMs = minutes * 60 * 1000L;
        String query = String.format(
                "SELECT \"%s\" FROM \"%s\" WHERE \"service\" = '%s' " +
                        "AND time > now() - %dms ORDER BY time ASC",
                field, measurement, serviceName, windowMs);

        QueryResult result = influxDB.query(new Query(query, db));
        return extractValuesFromResult(result);
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
        List<Double> recentMetrics = getMetricsInTimeWindow(
                measurement, field, serviceName, predictionWindowMinutes);

        if (recentMetrics.isEmpty()) {
            return currentPods;
        }

        KalmanFilter kf = new KalmanFilter(0.01, 0.1);
        for (double metric : recentMetrics) {
            kf.update(metric);
        }

        double predictedLoad = kf.predictNext();
        double safetyFactor = 1.2;
        double effectiveCapacity = podCapacity * safetyFactor;

        int requiredPods = (int) Math.ceil(predictedLoad / effectiveCapacity);
        requiredPods = Math.max(minPods, Math.min(maxPods, requiredPods));

        if (requiredPods < currentPods) {
            double utilizationThreshold = 0.7;
            if (predictedLoad > (currentPods - 1) * podCapacity * utilizationThreshold) {
                return currentPods;
            }
        }

        return requiredPods;
    }

    private List<Double> extractValuesFromResult(QueryResult result) {
        List<Double> values = new ArrayList<>();
        result.getResults().stream()
                .filter(r -> r.getSeries() != null)
                .flatMap(r -> r.getSeries().stream())
                .filter(s -> s.getValues() != null)
                .flatMap(s -> s.getValues().stream())
                .filter(v -> v.size() > 1)
                .map(v -> v.get(1))
                .filter(Number.class::isInstance)
                .map(n -> ((Number) n).doubleValue())
                .forEach(values::add);
        return values;
    }

    private Map<Instant, Double> extractTimestampsAndValuesFromResult(QueryResult result) {
        Map<Instant, Double> timeValueMap = new LinkedHashMap<>();
        result.getResults().stream()
                .filter(r -> r.getSeries() != null)
                .flatMap(r -> r.getSeries().stream())
                .filter(s -> s.getValues() != null)
                .flatMap(s -> s.getValues().stream())
                .filter(v -> v.size() > 1)
                .forEach(v -> {
                    Instant time = Instant.parse(v.get(0).toString());
                    Double value = ((Number) v.get(1)).doubleValue();
                    timeValueMap.put(time, value);
                });
        return timeValueMap;
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
}