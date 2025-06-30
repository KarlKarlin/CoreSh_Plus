package org.CorePlane.services;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class MetricsProcessing {

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

    public Map<Instant, Double> getMetricsWithTimestampsInWindow(String measurement,
                                                                 String field,
                                                                 String serviceName,
                                                                 int minutes) {
        long windowMs = minutes * 60 * 1000L;
        String query = String.format(
                "SELECT \"%s\" FROM \"%s\" WHERE \"service\" = '%s' " +
                        "AND time > now() - %dms ORDER BY time ASC",
                field, measurement, serviceName, windowMs);

        QueryResult result = influxDB.query(new Query(query, db));
        return extractTimestampsAndValuesFromResult(result);
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


}