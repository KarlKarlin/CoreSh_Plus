package org.CorePlane.configurations;

import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.Yaml;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ConfigProcessing {

    private static final String CONFIGURATION_FILE = System.getenv("CONFIG_DIR") + "/coresh-config.yaml";
    private static final int MAX_SERVICES = 50;

    private Map<String, Object> getConfigFromYaml() {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Paths.get(CONFIGURATION_FILE))) {
            return yaml.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load YAML configuration from " + CONFIGURATION_FILE, e);
        }
    }

    public List<String> getDockerServices() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> docker = (Map<String, Object>) data.getOrDefault("Docker", new HashMap<>());
        String servicesString = (String) docker.getOrDefault("services", "");

        if (servicesString.isEmpty()) {
            return Collections.emptyList();
        }

        String[] servicesArray = servicesString.split("/");
        return Arrays.asList(servicesArray).subList(0, Math.min(servicesArray.length, MAX_SERVICES));
    }

    public List<String> getServicesForTracer() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> jaeger = (Map<String, Object>) data.getOrDefault("Jaeger", new HashMap<>());
        String servicesString = (String) jaeger.getOrDefault("services", "");

        if (servicesString.isEmpty()) {
            return Collections.emptyList();
        }

        String[] servicesArray = servicesString.split("/");
        return Arrays.asList(servicesArray).subList(0, Math.min(servicesArray.length, MAX_SERVICES));
    }

    public int getAnalysisPercentage() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> jaeger = (Map<String, Object>) data.getOrDefault("Jaeger", new HashMap<>());
        return (int) jaeger.getOrDefault("analysisPercentage", "");
    }

    public int getWindowMinutesForJaeger() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> jaeger = (Map<String, Object>) data.getOrDefault("Jaeger", new HashMap<>());
        return (int) jaeger.getOrDefault("windowMinutes", "");
    }

    public int getWindowMinutesForLogs() {
        Map<String, Object> data = getConfigFromYaml();
        return (int) data.getOrDefault("window_minutes_for_logs_research", 5);
    }

    public String getMainJaegerService() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> jaeger = (Map<String, Object>) data.getOrDefault("Jaeger", new HashMap<>());
        return (String) jaeger.getOrDefault("mainService", "");
    }

    public double getMaxLatencyRate() {
        Map<String, Object> data = getConfigFromYaml();
        Map<String, Object> jaeger = (Map<String, Object>) data.getOrDefault("Jaeger", new HashMap<>());
        return (double) jaeger.getOrDefault("maxLatencyRate", "");
    }

    public int getMaxReplicasForService(String serviceName) {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> servicesConfig = (Map<String, Object>) config.getOrDefault("services_config", new HashMap<>());
        Map<String, Object> serviceConfig = (Map<String, Object>) servicesConfig.getOrDefault(serviceName, new HashMap<>());

        return (int) serviceConfig.getOrDefault("max_replicas", 10);
    }

    public int getMinReplicasForService(String serviceName) {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> servicesConfig = (Map<String, Object>) config.getOrDefault("services_config", new HashMap<>());
        Map<String, Object> serviceConfig = (Map<String, Object>) servicesConfig.getOrDefault(serviceName, new HashMap<>());

        return (int) serviceConfig.getOrDefault("min_replicas", 1);
    }

    public long getUpdateExpire() {
        Map<String, Object> data = getConfigFromYaml();
        Object value = data.get("updateExpire");

        if (value == null) {
            return 24L;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("updateExpire must be a valid number", e);
            }
        }

        throw new RuntimeException("updateExpire must be a number or numeric string");
    }

    public int getMaxErrorRate(){
        Map<String, Object> data = getConfigFromYaml();
        return (int) data.get("maxErrorRate");
    }

    public List<String> getMonitoredMetrics() {
        Map<String, Object> config = getConfigFromYaml();
        return (List<String>) config.getOrDefault("monitored_metrics",
                List.of("cpu.usage_percent", "memory.used_percent"));
    }

    public boolean isAutoPredictsAvailable(){
        Map<String, Object> data = getConfigFromYaml();
        return (boolean) data.getOrDefault("auto_predicts_available", false);
    }

    public int getErrorWindowHours() {
        Map<String, Object> data = getConfigFromYaml();
        return (int) data.get("error_window_minutes") / 60;
    }

    public boolean isLogTracker() {
        Map<String, Object> data = getConfigFromYaml();
        return (boolean) data.getOrDefault("log_tracker", false);
    }

    public int unitsPerPod(){
        Map<String, Object> data = getConfigFromYaml();
        return (int) data.getOrDefault("units_per_pod", 30);
    }

    public int getMetricWindowMinutes(String metric) {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> metricsConfig = (Map<String, Object>) config.getOrDefault("metrics_config", new HashMap<>());
        return (int) metricsConfig.getOrDefault(metric + ".window_minutes", 5);
    }

    public int getSpikeNotificationThreshold() {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> alertConfig = (Map<String, Object>) config.getOrDefault("alert_config", new HashMap<>());
        return (int) alertConfig.getOrDefault("spike_notification_threshold", 10);
    }

    public double getMetricWarningThreshold(String metric) {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> thresholds = (Map<String, Object>) config.getOrDefault("metric_thresholds", new HashMap<>());
        Map<String, Object> metricThresholds = (Map<String, Object>) thresholds.getOrDefault(metric, new HashMap<>());
        return (double) metricThresholds.getOrDefault("warning", 80.0);
    }

    public double getMetricCriticalThreshold(String metric) {
        Map<String, Object> config = getConfigFromYaml();
        Map<String, Object> thresholds = (Map<String, Object>) config.getOrDefault("metric_thresholds", new HashMap<>());
        Map<String, Object> metricThresholds = (Map<String, Object>) thresholds.getOrDefault(metric, new HashMap<>());
        return (double) metricThresholds.getOrDefault("critical", 90.0);
    }
}