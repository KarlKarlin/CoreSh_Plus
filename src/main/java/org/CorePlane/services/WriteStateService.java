package org.CorePlane.services;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.model.Service;
import com.github.dockerjava.api.model.Task;
import com.github.dockerjava.api.model.TaskState;
import org.CorePlane.configurations.ConfigProcessing;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.springframework.beans.factory.annotation.Value;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class WriteStateService {
    @Value("${influx.url}") private String influxUrl;
    @Value("${influx.username}") private String username;
    @Value("${influx.password}") private String password;

    private final List<String> criticalStates = Arrays.asList(
            "ERROR", "NOT_FOUND", "FAILED", "REJECTED", "ORPHANED", "CRASHED"
    );

    private final List<String> transitionalStates = Arrays.asList(
            "NEW", "PENDING", "ASSIGNED", "PREPARING", "STARTING",
            "UPDATING", "ROLLBACK", "PAUSED"
    );

    private final String DATABASE_NAME = "docker_monitoring";
    private final String RETENTION_POLICY = "autogen";
    private final String MEASUREMENT_NAME = "service_states";

    private final DockerClient dockerClient;
    private final ConfigProcessing configProcessing;
    private final RedisService stateHistoryService;

    public WriteStateService(DockerClient dockerClient,
                             ConfigProcessing configProcessing,
                             RedisService stateHistoryService) {
        this.dockerClient = dockerClient;
        this.configProcessing = configProcessing;
        this.stateHistoryService = stateHistoryService;
    }

    public void checkAndWriteState(String serviceName) {
        if (!stateHistoryService.acquireLock(serviceName)) {
            return;
        }

        try {
            String currentState = determineCurrentState(serviceName);
            String lastState = stateHistoryService.getLastState(serviceName);

            if (lastState != null) {
                stateHistoryService.saveStateTransition(serviceName, lastState, currentState);
            } else {
                stateHistoryService.saveStateTransition(serviceName, "INITIAL", currentState);
            }

            String predictedState = predictNextState(serviceName, currentState);
            checkForCriticalState(serviceName, currentState, predictedState);
            writeServiceStateToInflux(serviceName, currentState, predictedState);
        } finally {
            stateHistoryService.releaseLock(serviceName);
        }
    }

    public List<Map<String, String>> getServiceStatesWithPredictions() {
        return configProcessing.getDockerServices().stream()
                .map(serviceName -> {
                    Map<String, String> serviceState = new LinkedHashMap<>();
                    serviceState.put("serviceName", serviceName);
                    try {
                        String currentState = determineCurrentState(serviceName);
                        serviceState.put("currentState", currentState);
                        serviceState.put("predictedState", predictNextState(serviceName, currentState));
                    } catch (Exception e) {
                        serviceState.put("currentState", "ERROR");
                        serviceState.put("predictedState", "ERROR");
                    }
                    return serviceState;
                })
                .collect(Collectors.toList());
    }

    private String determineCurrentState(String serviceName) {
        try {
            List<Service> services = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec();

            if (services.isEmpty()) return "NOT_FOUND";

            Service service = services.get(0);
            List<Task> tasks = dockerClient.listTasksCmd()
                    .withServiceFilter(serviceName)
                    .exec();

            Map<TaskState, Long> stateCounts = tasks.stream()
                    .collect(Collectors.groupingBy(
                            t -> t.getStatus().getState(),
                            Collectors.counting()
                    ));

            long desiredReplicas = service.getSpec().getMode().getReplicated().getReplicas();
            long runningCount = stateCounts.getOrDefault(TaskState.RUNNING, 0L);

            if (runningCount == 0) {
                if (desiredReplicas == 0) {
                    return "SHUTDOWN";
                }
                if (stateCounts.containsKey(TaskState.FAILED)) {
                    return "FAILED";
                }
                return "DOWN";
            }

            if (runningCount < desiredReplicas) {
                return "DEGRADED";
            }

            if (service.getUpdateStatus() != null) {
                String updateState = service.getUpdateStatus().getState().toString().toUpperCase();
                if (!"COMPLETED".equalsIgnoreCase(updateState)) {
                    return "UPDATING";
                }
            }

            if (service.getSpec().getRollbackConfig() != null) {
                return "ROLLBACK";
            }

            return "RUNNING";
        } catch (Exception e) {
            return "ERROR";
        }
    }

    private String predictNextState(String serviceName, String currentState) {
        if (transitionalStates.contains(currentState)) {
            return stateHistoryService.getStateHistory(serviceName).contains("FAILED")
                    ? "FAILED"
                    : "RUNNING";
        }

        if (isCriticalState(currentState)) {
            return currentState;
        }

        Map<String, Integer> transitions = stateHistoryService.getTransitionProbabilities(
                serviceName,
                currentState
        );

        return transitions.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(currentState);
    }

    private boolean isCriticalState(String state) {
        return criticalStates.contains(state);
    }

    private void checkForCriticalState(String serviceName, String currentState, String predictedState) {
        if (isCriticalState(currentState)) {
            triggerAlert(serviceName, currentState, predictedState, "CRITICAL");
        } else if (isCriticalState(predictedState)) {
            triggerAlert(serviceName, currentState, predictedState, "WARN");
        }
    }

    private void triggerAlert(String serviceName, String currentState,
                              String predictedState, String severity) {
        String message = String.format(
                "Service Alert:%nService: %s%nCurrent State: %s%nPredicted State: %s%n" +
                        "State History: %s%nSeverity: %s%nTime: %s",
                serviceName,
                currentState,
                predictedState,
                String.join(" → ", stateHistoryService.getStateHistory(serviceName)),
                severity,
                new Date()
        );

        stateHistoryService.commentProblem(serviceName, message, severity.toLowerCase());
    }

    private void writeServiceStateToInflux(String serviceName, String state, String predictedState) {
        try (InfluxDB influxDB = InfluxDBFactory.connect(influxUrl, username, password)) {
            if (!influxDB.databaseExists(DATABASE_NAME)) {
                influxDB.createDatabase(DATABASE_NAME);
            }

            influxDB.setDatabase(DATABASE_NAME);
            influxDB.setRetentionPolicy(RETENTION_POLICY);

            Point point = Point.measurement(MEASUREMENT_NAME)
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .tag("service", serviceName)
                    .addField("state", state)
                    .addField("predicted_state", predictedState)
                    .addField("is_critical", isCriticalState(state) ? 1 : 0)
                    .addField("replicas", getReplicaCount(serviceName))
                    .build();

            influxDB.write(point);
        } catch (Exception e) {
            System.err.println("Error writing to InfluxDB: " + e.getMessage());
        }
    }

    private long getReplicaCount(String serviceName) {
        try {
            return dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec()
                    .stream()
                    .findFirst()
                    .map(s -> s.getSpec().getMode().getReplicated().getReplicas())
                    .orElse(0L);
        } catch (Exception e) {
            return -1L;
        }
    }
}