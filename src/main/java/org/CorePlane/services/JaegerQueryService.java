package org.CorePlane.services;

import com.fasterxml.jackson.core.type.TypeReference;
import org.CorePlane.configurations.ConfigProcessing;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.*;
import java.net.URI;
import java.util.stream.Collectors;

@Service
public class JaegerQueryService {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final HttpHeaders headers;
    private final ConfigProcessing configProcessing;
    private final RedisService redisService;

    @Value("${jaeger.url}")
    private String jaegerQueryUrl;

    public JaegerQueryService(RestTemplate restTemplate, ObjectMapper objectMapper, ConfigProcessing configProcessing, RedisService redisService) {
        this.restTemplate = restTemplate;
        this.objectMapper = objectMapper;
        this.configProcessing = configProcessing;
        this.redisService = redisService;
        this.headers = new HttpHeaders();
        this.headers.setAccept(List.of(MediaType.APPLICATION_JSON));
    }

    public ErrorTraceInfo getLastErrorTraceForService(String serviceName, long lookbackHours) {
        try {
            List<String> traceIds = fetchErrorTraceIdsForService(serviceName, lookbackHours, 1);
            if (traceIds.isEmpty()) {
                return new ErrorTraceInfo(null, serviceName, "No error traces found", null);
            }
            return fetchAndParseTraceDetails(traceIds.get(0));
        } catch (Exception e) {
            redisService.commentProblem(serviceName, "Failed to get error trace details for service", "notification");
            return new ErrorTraceInfo(null, serviceName, "Error fetching trace", e.getMessage());
        }
    }

    public List<String> fetchErrorTraceIdsForService(String serviceName, long lookbackHours, int limit) throws IOException {
        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", limit)
                .queryParam("lookback", lookbackHours + "h")
                .queryParam("tags", "{\"error\":\"true\"}")
                .build()
                .toUri();

        ResponseEntity<String> response = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        JsonNode rootNode = objectMapper.readTree(response.getBody());
        JsonNode dataNode = rootNode.path("data");

        return objectMapper.convertValue(dataNode, new TypeReference<List<Map<String, Object>>>() {})
                .stream()
                .map(trace -> (String) trace.get("traceID"))
                .collect(Collectors.toList());
    }

    private ErrorTraceInfo fetchAndParseTraceDetails(String traceId) throws IOException {
        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces/" + traceId)
                .build()
                .toUri();

        ResponseEntity<String> response = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        JsonNode rootNode = objectMapper.readTree(response.getBody());
        JsonNode traceData = rootNode.path("data").get(0);

        String serviceName = traceData.path("processes").path("p1").path("serviceName").asText();
        String operationName = null;
        String errorMessage = "Unknown error";
        String httpStatus = null;
        String exceptionClass = null;

        for (JsonNode span : traceData.path("spans")) {
            if (isErrorSpan(span)) {
                operationName = span.path("operationName").asText();

                for (JsonNode tag : span.path("tags")) {
                    String key = tag.path("key").asText();
                    String value = tag.path("value").asText();

                    switch (key) {
                        case "error.message":
                            errorMessage = value;
                            break;
                        case "http.status_code":
                            httpStatus = value;
                            break;
                        case "exception.class":
                            exceptionClass = value;
                            break;
                    }
                }

                String errorDetails = buildErrorDetails(exceptionClass, errorMessage, httpStatus);
                return new ErrorTraceInfo(traceId, serviceName, operationName, errorDetails);
            }
        }

        return new ErrorTraceInfo(traceId, serviceName, operationName, errorMessage);
    }

    private boolean isErrorSpan(JsonNode span) {
        for (JsonNode tag : span.path("tags")) {
            if ("error".equals(tag.path("key").asText()) &&
                    "true".equals(tag.path("value").asText())) {
                return true;
            }
        }
        return false;
    }

    private String buildErrorDetails(String exceptionClass, String errorMessage, String httpStatus) {
        StringBuilder details = new StringBuilder();
        if (exceptionClass != null) {
            details.append(exceptionClass).append(": ");
        }
        details.append(errorMessage);
        if (httpStatus != null) {
            details.append(" (HTTP ").append(httpStatus).append(")");
        }
        return details.toString();
    }

    public int getErrorTracesCount(String serviceName, long lookbackHours) {
        try {
            List<String> traceIds = fetchErrorTraceIdsForService(serviceName, lookbackHours, 100);
            return traceIds.size();
        } catch (Exception e) {
            redisService.commentProblem(serviceName, "Failed to get error trace details for service", "notification");
            return 0;
        }
    }

    public List<String> getTraceIdsForWaterfall() {
        try {

            String mainService = configProcessing.getMainJaegerService();
            int windowMinutes = configProcessing.getWindowMinutesForJaeger();
            int analysisPercentage = configProcessing.getAnalysisPercentage();

            if (mainService == null || mainService.isEmpty()) {
                throw new IllegalArgumentException("Main Jaeger service not configured");
            }

            double lookbackHours = windowMinutes / 60.0;

            int totalTraces = getTotalTracesCount(mainService, lookbackHours);

            int tracesToFetch = (int) Math.ceil(totalTraces * (analysisPercentage / 100.0));

            return fetchTraceIdsForService(mainService, lookbackHours, tracesToFetch);

        } catch (Exception e) {
            redisService.commentProblem("Unknown", "Failed to get trace IDs for waterfall analysis", "notification");
            return Collections.emptyList();
        }
    }

    private int getTotalTracesCount(String serviceName, double lookbackHours) throws IOException {

        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", 1)
                .queryParam("lookback", lookbackHours + "h")
                .build()
                .toUri();

        ResponseEntity<String> response = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        String totalCountHeader = response.getHeaders().getFirst("X-Total-Count");
        if (totalCountHeader != null) {
            return Integer.parseInt(totalCountHeader);
        }

        JsonNode rootNode = objectMapper.readTree(response.getBody());
        return rootNode.path("data").size();
    }

    private List<String> fetchTraceIdsForService(String serviceName, double lookbackHours, int limit) throws IOException {
        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", limit)
                .queryParam("lookback", lookbackHours + "h")
                .build()
                .toUri();

        ResponseEntity<String> response = restTemplate.exchange(
                uri,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        JsonNode rootNode = objectMapper.readTree(response.getBody());
        JsonNode dataNode = rootNode.path("data");

        return objectMapper.convertValue(dataNode, new TypeReference<List<Map<String, Object>>>() {})
                .stream()
                .map(trace -> (String) trace.get("traceID"))
                .collect(Collectors.toList());
    }

    public Map<String, Object> getWaterfallAnalysisMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        try {
            String mainService = configProcessing.getMainJaegerService();
            int windowMinutes = configProcessing.getWindowMinutesForJaeger();
            int analysisPercentage = configProcessing.getAnalysisPercentage();
            double lookbackHours = windowMinutes / 60.0;

            int totalTraces = getTotalTracesCount(mainService, lookbackHours);
            int sampledTraces = (int) Math.ceil(totalTraces * (analysisPercentage / 100.0));

            metrics.put("totalTraces", totalTraces);
            metrics.put("sampledTraces", sampledTraces);
            metrics.put("samplingPercentage", analysisPercentage);
            metrics.put("timeWindowMinutes", windowMinutes);
            metrics.put("mainService", mainService);

        } catch (Exception e) {
            redisService.commentProblem("Unknown", "Failed to get waterfall analysis metrics", "notification");
            metrics.put("error", e.getMessage());
        }
        return metrics;
    }

    public Optional<TraceSpan> getTraceSpan(String traceId, String serviceName) {
        try {
            URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                    .path("/api/traces/" + traceId)
                    .queryParam("service", serviceName)
                    .build()
                    .toUri();

            ResponseEntity<String> response = restTemplate.exchange(
                    uri,
                    HttpMethod.GET,
                    new HttpEntity<>(headers),
                    String.class
            );

            JsonNode rootNode = objectMapper.readTree(response.getBody());
            JsonNode traceData = rootNode.path("data").get(0);

            if (traceData == null || traceData.isNull()) {
                return Optional.empty();
            }

            for (JsonNode span : traceData.path("spans")) {
                String processId = span.path("processID").asText();
                JsonNode process = traceData.path("processes").path(processId);

                if (serviceName.equals(process.path("serviceName").asText())) {
                    long startTime = span.path("startTime").asLong();
                    long duration = span.path("duration").asLong();
                    String operationName = span.path("operationName").asText();

                    Map<String, String> tags = new HashMap<>();
                    for (JsonNode tag : span.path("tags")) {
                        tags.put(tag.path("key").asText(), tag.path("value").asText());
                    }

                    return Optional.of(new TraceSpan(
                            traceId,
                            serviceName,
                            operationName,
                            startTime,
                            duration,
                            tags
                    ));
                }
            }
        } catch (Exception e) {
            redisService.commentProblem("Unknown", "Failed to get waterfall analysis metrics", "notification");
        }
        return Optional.empty();
    }

    public record ErrorTraceInfo(
            String traceId,
            String serviceName,
            String operationName,
            String errorDetails
    ) {
        @NotNull
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (traceId != null) sb.append("Trace ID: ").append(traceId).append("\n");
            if (serviceName != null) sb.append("Service: ").append(serviceName).append("\n");
            if (operationName != null) sb.append("Operation: ").append(operationName).append("\n");
            if (errorDetails != null) sb.append("Error: ").append(errorDetails);
            return sb.toString();
        }
    }

    public static class TraceSpan {
        private final String traceId;
        private final String serviceName;
        private final String operationName;
        private final long startTime;
        private final double duration;
        private final Map<String, String> tags;

        public TraceSpan(String traceId, String serviceName, String operationName,
                         long startTime, double duration, Map<String, String> tags) {
            this.traceId = traceId;
            this.serviceName = serviceName;
            this.operationName = operationName;
            this.startTime = startTime;
            this.duration = duration;
            this.tags = new HashMap<>(tags);
        }

        public boolean isError() {
            return "true".equals(tags.get("error"));
        }

        public Map<String, String> getErrorTags() {
            return tags.entrySet().stream()
                    .filter(e -> e.getKey().startsWith("error.") || e.getKey().startsWith("http.status_code"))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public String getTraceId() { return traceId; }
        public String getServiceName() { return serviceName; }
        public String getOperationName() { return operationName; }
        public long getStartTime() { return startTime; }
        public double getDuration() { return duration; }
        public Map<String, String> getTags() { return Collections.unmodifiableMap(tags); }
    }
}