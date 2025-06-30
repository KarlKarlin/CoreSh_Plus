package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RedisService {

    private final DockerSwarmService dockerSwarmService;
    private final StringRedisTemplate stringRedisTemplate;
    private final ConfigProcessing configProcessing;

    private static final String STATE_HISTORY_KEY = "service:state:history:%s";
    private static final String STATE_TRANSITION_KEY = "service:state:transitions:%s:%s";
    private static final int MAX_HISTORY = 10;
    private static final Duration STATE_TTL = Duration.ofDays(7);

    private static final String LOCK_PREFIX = "lock:";
    private static final Duration LOCK_TIMEOUT = Duration.ofSeconds(30);
    private static final String PROBLEM_COMMENT_PREFIX = "problemCommenting:";

    private static final String VERSION_HISTORY_KEY = "service:versions:%s";
    private static final String LAST_DEPLOYMENT_KEY = "service:last_deploy:%s";
    private static final int MAX_VERSION_HISTORY = 10;

    public RedisService(DockerSwarmService dockerSwarmService, StringRedisTemplate stringRedisTemplate, ConfigProcessing configProcessing) {
        this.dockerSwarmService = dockerSwarmService;
        this.stringRedisTemplate = stringRedisTemplate;
        this.configProcessing = configProcessing;
    }

    public void saveBiggestValueForMetric(double biggestValue, String metric, String service) {
        stringRedisTemplate.opsForValue().set("metrics:biggestValue:" + metric + ":" + service, String.valueOf(biggestValue));
    }

    public double getBiggestValueForMetric(String metric, String service) {
        String biggestValue = stringRedisTemplate.opsForValue().get("metrics:biggestValue:" + metric + ":" + service);
        if (biggestValue != null) {
            return Double.parseDouble(biggestValue);
        }
        return 0;
    }

    public void clearBiggestValueForMetric(String metric, String service) {
        stringRedisTemplate.delete("metrics:biggestValue:" + metric + ":" + service);
    }

    public void trackServiceDeployment(String serviceName) {
        DockerSwarmService.ServiceVersion version = dockerSwarmService.getCurrentServiceVersion(serviceName);
        String historyKey = String.format(VERSION_HISTORY_KEY, serviceName);

        String versionEntry = String.format("%s|%s|%s",
                version.timestamp(),
                version.image(),
                version.metadata().entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(","))
        );

        List<String> existingVersions = stringRedisTemplate.opsForList().range(historyKey, 0, -1);

        String newImageVersion = version.image();

        boolean versionExists = false;
        if (existingVersions != null) {
            versionExists = existingVersions.stream()
                    .anyMatch(v -> {
                        String[] parts = v.split("\\|");
                        if (parts.length >= 2) {
                            String existingImageVersion = parts[1];
                            return existingImageVersion.equals(newImageVersion);
                        }
                        return false;
                    });
        }

        if (!versionExists) {
            stringRedisTemplate.opsForList().leftPush(historyKey, versionEntry);
            stringRedisTemplate.opsForList().trim(historyKey, 0, MAX_VERSION_HISTORY - 1);
        }

        stringRedisTemplate.opsForValue().set(
                String.format(LAST_DEPLOYMENT_KEY, serviceName),
                versionEntry,
                Duration.ofDays(7)
        );
    }

    public List<DockerSwarmService.ServiceVersion> getServiceVersionHistory(String serviceName) {
        String historyKey = String.format(VERSION_HISTORY_KEY, serviceName);
        List<String> versions = stringRedisTemplate.opsForList().range(historyKey, 0, -1);

        if (versions == null) return Collections.emptyList();

        return versions.stream()
                .map(this::parseVersionEntry)
                .collect(Collectors.toList());
    }

    private DockerSwarmService.ServiceVersion parseVersionEntry(String entry) {
        String[] parts = entry.split("\\|", 3);
        Map<String, String> metadata = Arrays.stream(parts[2].split(","))
                .map(p -> p.split("="))
                .collect(Collectors.toMap(a -> a[0], a -> a.length > 1 ? a[1] : ""));

        return new DockerSwarmService.ServiceVersion(
                parts[1],
                OffsetDateTime.parse(parts[0]),
                metadata
        );
    }

    public boolean rollbackToPreviousVersion(String serviceName) {
        try {
            List<DockerSwarmService.ServiceVersion> history = getServiceVersionHistory(serviceName);
            if (history.isEmpty() || history.size() == 1) {
                return false;
            }

            DockerSwarmService.ServiceVersion target = history.get(1);

            Duration age = Duration.between(target.timestamp(), OffsetDateTime.now());
            if (age.toHours() > configProcessing.getUpdateExpire()) {
                return false;
            }

            dockerSwarmService.rollbackToVersion(serviceName, target.image());

            return true;

        } catch (Exception e) {
            throw new RuntimeException("Rollback failed", e);
        }
    }

    public void setWithExpiry(String Id, String definition, long minutes) {
        stringRedisTemplate.opsForValue().set(Id, definition, Duration.ofMinutes(minutes));
    }

    public boolean exists(String Id) {
        return stringRedisTemplate.hasKey(Id);
    }

    public void setProtectedReplicasCount(int replicasCount, String serviceName) {
        if (replicasCount < 1) {
            throw new IllegalArgumentException("Replicas count must be more than 1 or 1");
        }

        String redisKey = "replicas:protected:" + serviceName;
        stringRedisTemplate.opsForValue().set(
                redisKey,
                String.valueOf(replicasCount),
                Duration.ofDays(1)
        );
    }

    public int getProtectedReplicasCount(String serviceName) {
        try {
            String redisKey = "replicas:protected:" + serviceName;
            String value = stringRedisTemplate.opsForValue().get(redisKey);
            return (value != null && !value.isBlank()) ?
                    Integer.parseInt(value) :
                    0;
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    public void setWLForService(String serviceName, double wl) {

        String key = String.format("wl:%s:%d", serviceName, System.currentTimeMillis());
        stringRedisTemplate.opsForValue().set(key, String.valueOf(wl), Duration.ofDays(8));
    }

    public List<Double> getAllWLForService(String serviceName) {

        Set<String> keys = stringRedisTemplate.keys("wl:" + serviceName + ":*");
        if (keys.isEmpty()) return Collections.emptyList();

        List<Long> timestamps = keys.stream()
                .map(key -> Long.parseLong(key.split(":")[2]))
                .sorted()
                .toList();

        return timestamps.stream()
                .map(ts -> String.format("wl:%s:%d", serviceName, ts))
                .map(key -> stringRedisTemplate.opsForValue().get(key))
                .filter(Objects::nonNull)
                .map(Double::valueOf)
                .collect(Collectors.toList());
    }

    public void addLtForDay(String servicename, double value) {
        stringRedisTemplate.opsForList().leftPush("Lt:" + servicename, String.valueOf(value));
    }

    public void clearLtForDay(String servicename) {
        stringRedisTemplate.delete("Lt:" + servicename);
    }

    public List<Double> getLtForDay(String servicename) {
        List<String> values = stringRedisTemplate.opsForList().range("Lt:" + servicename, 0, -1);
        if (values == null) {
            return Collections.emptyList();
        }
        return values.stream()
                .map(Double::valueOf)
                .collect(Collectors.toList());
    }

    public void setCooldown(String serviceName, String metric) {
        stringRedisTemplate.opsForValue().set("cooldown:" + serviceName, "frozen", Duration.ofMinutes(configProcessing.getMetricWindowMinutes(metric)));
    }

    public boolean isCooldown(String serviceName) {
        try {
            String cooldownStatus = stringRedisTemplate.opsForValue().get("cooldown:" + serviceName);
            return cooldownStatus != null && cooldownStatus.equals("frozen");
        } catch (Exception e) {
            return true;
        }
    }

    public void setCooldownForMetric(String serviceName, String metric) {
        stringRedisTemplate.opsForValue().set("cooldown:" + serviceName + ":" + metric, "frozen1", Duration.ofMinutes(configProcessing.getMetricWindowMinutes(metric)));
    }

    public boolean isCooldownForMetric(String serviceName, String metric) {
        try {
            String cooldownStatus = stringRedisTemplate.opsForValue().get("cooldown:" + serviceName + ":" + metric);
            return cooldownStatus != null && cooldownStatus.equals("frozen1");
        } catch (Exception e) {
            return true;
        }
    }

    public void saveStateTransition(String serviceName, String fromState, String toState) {

        String transitionKey = String.format(STATE_TRANSITION_KEY, serviceName, fromState);
        stringRedisTemplate.opsForHash().increment(transitionKey, toState, 1);
        stringRedisTemplate.expire(transitionKey, STATE_TTL);

        String historyKey = String.format(STATE_HISTORY_KEY, serviceName);
        stringRedisTemplate.opsForList().leftPush(historyKey, toState);

        if (stringRedisTemplate.opsForList().size(historyKey) > MAX_HISTORY) {
            stringRedisTemplate.opsForList().rightPop(historyKey);
        }
        stringRedisTemplate.expire(historyKey, STATE_TTL);
    }

    public List<String> getStateHistory(String serviceName) {
        String historyKey = String.format(STATE_HISTORY_KEY, serviceName);
        return stringRedisTemplate.opsForList().range(historyKey, 0, -1);
    }

    public Map<String, Integer> getTransitionProbabilities(String serviceName, String fromState) {
        String transitionKey = String.format(STATE_TRANSITION_KEY, serviceName, fromState);
        Map<Object, Object> rawMap = stringRedisTemplate.opsForHash().entries(transitionKey);

        return rawMap.entrySet().stream()
                .collect(Collectors.toMap(
                        e -> (String) e.getKey(),
                        e -> Integer.parseInt(e.getValue().toString())
                ));
    }

    public String getLastState(String serviceName) {
        String historyKey = String.format(STATE_HISTORY_KEY, serviceName);
        return stringRedisTemplate.opsForList().index(historyKey, 0);
    }

    public boolean acquireLock(String serviceName) {
        String lockKey = LOCK_PREFIX + serviceName;
        return Boolean.TRUE.equals(stringRedisTemplate.opsForValue().setIfAbsent(
                lockKey,
                "locked",
                LOCK_TIMEOUT
        ));
    }

    public void releaseLock(String serviceName) {
        stringRedisTemplate.delete(LOCK_PREFIX + serviceName);
    }

    public boolean hasLock(String serviceName) {
        return stringRedisTemplate.hasKey(LOCK_PREFIX + serviceName);
    }

    private String findFullKeyByUuid(String uuid) {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + uuid + ":*:*:*");
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("No problem found with UUID: " + uuid);
        }
        return keys.iterator().next();
    }

    public void commentProblem(String serviceName, String comment, String conditional) {
        String key = PROBLEM_COMMENT_PREFIX + UUID.randomUUID() + ":" +
                System.currentTimeMillis() + ":" +
                serviceName + ":" + conditional;
        stringRedisTemplate.opsForList().leftPush(key, comment);
    }

    public void addCommentToProblem(String uuid, String comment) {
        String fullKey = findFullKeyByUuid(uuid);

        stringRedisTemplate.opsForList().rightPush(fullKey, comment);
    }

    public String getProblem(String uuid) {
        String fullKey = findFullKeyByUuid(uuid);
        return stringRedisTemplate.opsForList().index(fullKey, 0);
    }

    public List<String> getComments(String uuid) {
        String fullKey = findFullKeyByUuid(uuid);
        return stringRedisTemplate.opsForList().range(fullKey, 1, -1);
    }

    public List<String> getProblemsByConditional(String conditional, int limit) {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*:*:*:" + conditional);

        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        return keys.stream()
                .map(key -> {
                    String[] parts = key.split(":");
                    if (parts.length >= 5) {
                        try {
                            return parts[1];
                        } catch (Exception e) {
                            return null;
                        }
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .limit(limit > 0 ? limit : Integer.MAX_VALUE)
                .collect(Collectors.toList());
    }

    public List<String> getAllProblemKeys() {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        return keys != null ? new ArrayList<>(keys) : Collections.emptyList();
    }

    public Map<String, Long> getConditionalStatistics() {
        Set<String> allKeys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        Map<String, Long> conditionalCounts = new HashMap<>();

        if (allKeys != null) {
            for (String key : allKeys) {
                String[] parts = key.split(":");
                if (parts.length >= 5) {
                    String conditional = parts[4];
                    conditionalCounts.merge(conditional, 1L, Long::sum);
                }
            }
        }

        return conditionalCounts;
    }

    public Map<String, Long> getConditionalStatisticsByService() {
        Set<String> allKeys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        Map<String, Long> serviceConditionalCounts = new HashMap<>();

        if (allKeys != null) {
            for (String key : allKeys) {
                String[] parts = key.split(":");
                if (parts.length >= 5) {
                    String service = parts[3];
                    String conditional = parts[4];
                    String compositeKey = service + ":" + conditional;
                    serviceConditionalCounts.merge(compositeKey, 1L, Long::sum);
                }
            }
        }

        return serviceConditionalCounts;
    }

    public Map<String, String> getProblemMetadata(String uuid) {
        String fullKey = findFullKeyByUuid(uuid);
        String[] parts = fullKey.split(":");
        if (parts.length < 5) {
            throw new IllegalArgumentException("Invalid problem key format");
        }
        return Map.of(
                "id", parts[1],
                "timestamp", parts[2],
                "serviceName", parts[3],
                "conditional", parts[4]
        );
    }

    public List<Map<String, String>> getAllProblemsWithMetadata() {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        if (keys == null) return Collections.emptyList();

        return keys.stream().map(key -> {
            Map<String, String> metadata = getProblemMetadata(key);
            metadata.put("description", stringRedisTemplate.opsForList().index(key, 0));
            metadata.put("commentCount", String.valueOf(stringRedisTemplate.opsForList().size(key) - 1));
            return metadata;
        }).collect(Collectors.toList());
    }

    public List<Map<String, String>> getRecentProblems(int limit) {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }

        return keys.stream()
                .map(key -> {
                    Map<String, String> problem = new LinkedHashMap<>();

                    String[] parts = key.split(":");
                    if (parts.length >= 5) {
                        problem.put("id", parts[1]);
                        problem.put("timestamp", parts[2]);
                        problem.put("serviceName", parts[3]);
                        problem.put("conditional", parts[4]);
                    }

                    String description = stringRedisTemplate.opsForList().index(key, 0);
                    problem.put("description", description != null ? description : "");

                    Long size = stringRedisTemplate.opsForList().size(key);
                    problem.put("commentCount", String.valueOf(size != null ? Math.max(0, size - 1) : 0));
                    return problem;
                })
                .sorted((m1, m2) -> {
                    long time1 = Long.parseLong(m1.getOrDefault("timestamp", "0"));
                    long time2 = Long.parseLong(m2.getOrDefault("timestamp", "0"));
                    return Long.compare(time2, time1);
                })
                .limit(limit)
                .collect(Collectors.toList());
    }

    public long getTotalCommentCount() {
        Set<String> keys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        if (keys == null) return 0;

        return keys.stream()
                .mapToLong(key -> Math.max(0, stringRedisTemplate.opsForList().size(key) - 1))
                .sum();
    }

    public void cleanupOldProblems(long olderThanMillis) {
        Set<String> allKeys = stringRedisTemplate.keys(PROBLEM_COMMENT_PREFIX + "*");
        if (allKeys != null) {
            long currentTime = System.currentTimeMillis();
            allKeys.forEach(key -> {
                String[] parts = key.split(":");
                if (parts.length >= 3) {
                    try {
                        long timestamp = Long.parseLong(parts[2]);
                        if (currentTime - timestamp > olderThanMillis) {
                            stringRedisTemplate.delete(key);
                        }
                    } catch (NumberFormatException e) {
                    }
                }
            });
        }
    }
}