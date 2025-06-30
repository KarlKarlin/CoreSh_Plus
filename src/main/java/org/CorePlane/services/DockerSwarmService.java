package org.CorePlane.services;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Service;
import com.github.dockerjava.api.model.ServiceSpec;
import com.github.dockerjava.api.model.TaskSpec;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@org.springframework.stereotype.Service
public class DockerSwarmService {

    private final DockerClient dockerClient;

    public DockerSwarmService(DockerClient dockerClient) {
        this.dockerClient = dockerClient;
    }

    public int getCurrentReplicasForService(String serviceName) {
        try {
            Optional<Long> replicas = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec()
                    .stream()
                    .findFirst()
                    .map(service -> service.getSpec().getMode().getReplicated().getReplicas());

            return replicas.orElse(0L).intValue();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get service replicas", e);
        }
    }

    public void scaleUpService(String serviceName, int replicas) {
        try {
            List<Service> services = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec();

            if (services.isEmpty()) {
                throw new IllegalArgumentException("Service " + serviceName + " not found");
            }

            Service service = services.get(0);
            ServiceSpec spec = service.getSpec();
            spec.getMode().getReplicated().withReplicas(replicas);

            dockerClient.updateServiceCmd(service.getId(), spec)
                    .withVersion(service.getVersion().getIndex())
                    .exec();

        } catch (Exception e) {
            throw new RuntimeException("Failed to scale service", e);
        }
    }

    public OffsetDateTime getLastDeploymentDateForService(String serviceName) {
        try {
            return dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec()
                    .stream()
                    .findFirst()
                    .map(service -> {
                        long updatedAt = service.getUpdatedAt().getSeconds() * 1000;
                        return OffsetDateTime.ofInstant(Instant.ofEpochMilli(updatedAt), ZoneId.systemDefault());
                    })
                    .orElseThrow(() -> new IllegalArgumentException("Service not found"));
        } catch (Exception e) {
            throw new RuntimeException("Failed to get deployment date", e);
        }
    }

    public boolean rollbackService(String serviceName, String previousImageTag) {
        try {
            List<Service> services = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec();

            if (services.isEmpty()) {
                throw new IllegalArgumentException("Service " + serviceName + " not found");
            }

            Service service = services.get(0);
            ServiceSpec spec = service.getSpec();
            TaskSpec taskSpec = spec.getTaskTemplate();

            String currentImage = taskSpec.getContainerSpec().getImage();
            String newImage = currentImage.split(":")[0] + ":" + previousImageTag;
            taskSpec.getContainerSpec().withImage(newImage);

            dockerClient.updateServiceCmd(service.getId(), spec)
                    .withVersion(service.getVersion().getIndex())
                    .exec();

            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to rollback service", e);
        }
    }

    public record ServiceVersion(
            String image,
            OffsetDateTime timestamp,
            Map<String, String> metadata
    ) {}

    public ServiceVersion getCurrentServiceVersion(String serviceName) {
        Service service = dockerClient.listServicesCmd()
                .withNameFilter(List.of(serviceName))
                .exec()
                .get(0);

        long updatedAtMillis = service.getUpdatedAt().getSeconds() * 1000;
        OffsetDateTime timestamp = OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(updatedAtMillis),
                ZoneId.systemDefault()
        );

        return new ServiceVersion(
                service.getSpec().getTaskTemplate().getContainerSpec().getImage(),
                timestamp,
                Map.of(
                        "replicas", String.valueOf(service.getSpec().getMode().getReplicated().getReplicas()),
                        "serviceId", service.getId()
                )
        );
    }

    public boolean rollbackToVersion(String serviceName, String targetImage) {
        try {

            ServiceVersion current = getCurrentServiceVersion(serviceName);
            if (current.image().equals(targetImage)) {
                throw new IllegalArgumentException("Service is already on this version");
            }

            return rollbackService(serviceName, targetImage.contains(":") ?
                    targetImage.split(":")[1] : "latest");
        } catch (Exception e) {
            throw new RuntimeException("Rollback failed", e);
        }
    }

    public List<String> getServiceLogsInTimeWindow(String serviceName, long timestampMillis, int windowMinutes) {
        try {
            List<Service> services = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec();

            if (services.isEmpty()) {
                throw new IllegalArgumentException("Service " + serviceName + " not found");
            }
            String serviceId = services.get(0).getId();

            Instant centerTime = Instant.ofEpochMilli(timestampMillis);
            Instant startTime = centerTime.minus(windowMinutes, ChronoUnit.MINUTES);
            Instant endTime = centerTime.plus(windowMinutes, ChronoUnit.MINUTES);

            List<String> logs = new ArrayList<>();
            CountDownLatch latch = new CountDownLatch(1);

            dockerClient.logServiceCmd(serviceId)
                    .withStdout(true)
                    .withStderr(true)
                    .withSince((int) startTime.getEpochSecond())
                    .withTimestamps(true)
                    .exec(new ResultCallback.Adapter<Frame>() {
                        @Override
                        public void onNext(Frame frame) {
                            String logEntry = new String(frame.getPayload());

                            try {
                                String[] parts = logEntry.split(" ", 2);
                                if (parts.length >= 2) {
                                    Instant logTime = Instant.parse(parts[0]);
                                    if (!logTime.isBefore(startTime) && !logTime.isAfter(endTime)) {
                                        logs.add(logEntry);
                                    }
                                } else {
                                    logs.add(logEntry);
                                }
                            } catch (Exception e) {
                                logs.add(logEntry);
                            }
                        }

                        @Override
                        public void onComplete() {
                            latch.countDown();
                        }

                        @Override
                        public void onError(Throwable throwable) {
                            latch.countDown();
                        }
                    });

            latch.await(30, TimeUnit.SECONDS);

            return logs.stream()
                    .filter(log -> {
                        try {
                            String[] parts = log.split(" ", 2);
                            if (parts.length >= 2) {
                                Instant logTime = Instant.parse(parts[0]);
                                return !logTime.isBefore(startTime) && !logTime.isAfter(endTime);
                            }
                            return true;
                        } catch (Exception e) {
                            return true;
                        }
                    })
                    .collect(Collectors.toList());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Log collection interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get service logs", e);
        }
    }

    public Map<String, List<String>> identifyErrorTasks(String serviceName, long timestampMillis, int windowMinutes) {
        try {
            List<String> logs = getServiceLogsInTimeWindow(serviceName, timestampMillis, windowMinutes);
            Map<String, List<String>> errorTasks = new LinkedHashMap<>();

            for (String log : logs) {
                if (isErrorLog(log)) {
                    String errorDescription = getFirstWords(log, 5);
                    errorTasks.computeIfAbsent(errorDescription, k -> new ArrayList<>()).add(log);
                }
            }

            return errorTasks;
        } catch (Exception e) {
            throw new RuntimeException("Failed to identify error tasks", e);
        }
    }

    private boolean isErrorLog(String log) {
        String lowerLog = log.toLowerCase();
        return lowerLog.contains("error") ||
                lowerLog.contains("exception") ||
                lowerLog.contains("fail");
    }

    private String getFirstWords(String text, int wordCount) {
        String[] words = text.split("\\s+");
        return Arrays.stream(words)
                .limit(wordCount)
                .collect(Collectors.joining(" "));
    }

    public boolean restartCrashedContainers(String serviceName) {
        try {

            List<String> crashedContainerIds = getCrashedContainerIds(serviceName);

            if (crashedContainerIds.isEmpty()) {
                return false;
            }

            for (String containerId : crashedContainerIds) {
                try {
                    dockerClient.removeContainerCmd(containerId)
                            .withForce(true)
                            .exec();
                    System.out.println("Removed crashed container: " + containerId);
                } catch (Exception e) {
                    return false;
                }
            }

            return true;
        } catch (Exception e) {
            throw new RuntimeException("Failed to restart crashed containers", e);
        }
    }

    private List<String> getCrashedContainerIds(String serviceName) {
        try {
            String serviceId = dockerClient.listServicesCmd()
                    .withNameFilter(Collections.singletonList(serviceName))
                    .exec()
                    .stream()
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Service not found: " + serviceName))
                    .getId();

            return dockerClient.listTasksCmd()
                    .withServiceFilter(serviceId)
                    .exec()
                    .stream()
                    .filter(task -> {
                        String state = task.getStatus().getState().toString().toLowerCase();
                        Integer exitCode = Optional.ofNullable(task.getStatus().getContainerStatus())
                                .map(status -> status.getExitCode())
                                .orElse(0);

                        return state.equals("failed") ||
                                state.equals("rejected") ||
                                (state.equals("exited") && exitCode != 0);
                    })
                    .map(task -> task.getStatus().getContainerStatus().getContainerID())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to get crashed container IDs", e);
        }
    }
}