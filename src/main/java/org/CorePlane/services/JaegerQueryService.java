package org.CorePlane.services;

import com.fasterxml.jackson.core.type.TypeReference;
import org.CorePlane.configurations.ConfigProcessing;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger logger = LoggerFactory.getLogger(JaegerQueryService.class);
    private static final long MICROS_PER_SECOND = 1_000_000L;
    private static final long MICROS_PER_MINUTE = 60 * MICROS_PER_SECOND;
    private static final long MICROS_PER_HOUR = 60 * MICROS_PER_MINUTE;

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;
    private final HttpHeaders headers;
    private final ConfigProcessing configProcessing;
    private final RedisService redisService;

    @Value("${jaeger.url}")
    private String jaegerQueryUrl;

    public JaegerQueryService(RestTemplate restTemplate, ObjectMapper objectMapper,
                              ConfigProcessing configProcessing, RedisService redisService) {
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
            logger.error("Failed to get last error trace for service: {}", serviceName, e);
            redisService.commentProblem(serviceName, "Failed to get error trace details for service", "notification");
            return new ErrorTraceInfo(null, serviceName, "Error fetching trace", e.getMessage());
        }
    }

    public List<String> fetchErrorTraceIdsForService(String serviceName, long lookbackHours, int limit) throws IOException {
        long lookbackMicros = lookbackHours * MICROS_PER_HOUR;
        long endTimeMicros = System.currentTimeMillis() * 1000;
        long startTimeMicros = endTimeMicros - lookbackMicros;

        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", limit)
                .queryParam("start", startTimeMicros)
                .queryParam("end", endTimeMicros)
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
        if (!rootNode.has("data")) {
            throw new IOException("Invalid response format from Jaeger - missing data field");
        }

        JsonNode dataNode = rootNode.path("data");
        return objectMapper.convertValue(dataNode, new TypeReference<List<Map<String, Object>>>() {})
                .stream()
                .map(trace -> (String) trace.get("traceID"))
                .filter(Objects::nonNull)
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

        if (traceData == null) {
            return new ErrorTraceInfo(traceId, "unknown", "No trace data found", null);
        }

        // Get first process's service name
        Iterator<Map.Entry<String, JsonNode>> processes = traceData.path("processes").fields();
        String serviceName = processes.hasNext() ?
                processes.next().getValue().path("serviceName").asText() : "unknown";

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

    private boolean isErrorSpan(JsonNode span) {
        for (JsonNode tag : span.path("tags")) {
            String key = tag.path("key").asText();
            String value = tag.path("value").asText();

            if (("error".equals(key) && "true".equals(value)) ||
                    (key.startsWith("error.")) ||
                    ("http.status_code".equals(key) && value.matches("5\\d\\d"))) {
                return true;
            }
        }
        return false;
    }

    public List<String> getAllTracesForService(String serviceName, long lookbackHours) throws IOException {
        long lookbackMicros = lookbackHours * MICROS_PER_HOUR;
        long endTimeMicros = System.currentTimeMillis() * 1000;
        long startTimeMicros = endTimeMicros - lookbackMicros;

        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("start", startTimeMicros)
                .queryParam("end", endTimeMicros)
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
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public int getErrorTracesCount(String serviceName, long lookbackHours) {
        try {
            List<String> traceIds = fetchErrorTraceIdsForService(serviceName, lookbackHours, 100);
            return traceIds.size();
        } catch (Exception e) {
            logger.error("Failed to get error traces count for service: {}", serviceName, e);
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

            return fetchTraceIdsForService(mainService, lookbackHours, Math.max(1, tracesToFetch));
        } catch (Exception e) {
            logger.error("Failed to get trace IDs for waterfall analysis", e);
            redisService.commentProblem("Unknown", "Failed to get trace IDs for waterfall analysis", "notification");
            return Collections.emptyList();
        }
    }

    private List<String> fetchTraceIdsForService(String serviceName, double lookbackHours, int limit) throws IOException {
        long lookbackMicros = (long)(lookbackHours * MICROS_PER_HOUR);
        long endTimeMicros = System.currentTimeMillis() * 1000;
        long startTimeMicros = endTimeMicros - lookbackMicros;

        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", limit)
                .queryParam("start", startTimeMicros)
                .queryParam("end", endTimeMicros)
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
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private int getTotalTracesCount(String serviceName, double lookbackHours) throws IOException {
        long lookbackMicros = (long)(lookbackHours * MICROS_PER_HOUR);
        long endTimeMicros = System.currentTimeMillis() * 1000;
        long startTimeMicros = endTimeMicros - lookbackMicros;

        URI uri = UriComponentsBuilder.fromHttpUrl(jaegerQueryUrl)
                .path("/api/traces")
                .queryParam("service", serviceName)
                .queryParam("limit", 1)
                .queryParam("start", startTimeMicros)
                .queryParam("end", endTimeMicros)
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

    public List<TraceSpan> getCompleteTrace(String traceId) throws IOException {
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
        if (rootNode == null || !rootNode.has("data") || !rootNode.get("data").isArray()) {
            throw new IOException("Invalid trace data format from Jaeger");
        }

        JsonNode traceData = rootNode.path("data").get(0);
        if (traceData == null) {
            return Collections.emptyList();
        }

        Map<String, String> processIdToService = new HashMap<>();
        traceData.path("processes").fields().forEachRemaining(entry -> {
            processIdToService.put(entry.getKey(), entry.getValue().path("serviceName").asText());
        });

        List<TraceSpan> spans = new ArrayList<>();
        for (JsonNode span : traceData.path("spans")) {
            String processId = span.path("processID").asText();
            String serviceName = processIdToService.get(processId);
            if (serviceName == null) continue;

            String parentSpanId = null;
            JsonNode references = span.path("references");
            if (references.size() > 0) {
                for (JsonNode ref : references) {
                    if ("CHILD_OF".equals(ref.path("refType").asText())) {
                        parentSpanId = ref.path("spanID").asText();
                        break;
                    }
                }
            }

            Map<String, String> tags = new HashMap<>();
            span.path("tags").forEach(tag -> {
                String key = tag.path("key").asText();
                JsonNode valueNode = tag.path("value");
                String value = valueNode.isValueNode() ? valueNode.asText() : valueNode.toString();
                tags.put(key, value);
            });

            spans.add(new TraceSpan(
                    traceId,
                    serviceName,
                    span.path("operationName").asText(),
                    span.path("startTime").asLong(),
                    span.path("duration").asLong(),
                    tags,
                    parentSpanId,
                    span.path("spanID").asText()
            ));
        }

        return spans;
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
        private final String parentSpanId;
        private final String spanId;

        public TraceSpan(String traceId, String serviceName, String operationName,
                         long startTime, double duration, Map<String, String> tags,
                         String parentSpanId, String spanId) {
            this.traceId = traceId;
            this.serviceName = serviceName;
            this.operationName = operationName;
            this.startTime = startTime;
            this.duration = duration;
            this.tags = new HashMap<>(tags);
            this.parentSpanId = parentSpanId;
            this.spanId = spanId;
        }

        public boolean isError() {
            return "true".equals(tags.get("error")) ||
                    tags.keySet().stream().anyMatch(k -> k.startsWith("error.")) ||
                    tags.entrySet().stream()
                            .filter(e -> "http.status_code".equals(e.getKey()))
                            .anyMatch(e -> e.getValue().matches("5\\d\\d"));
        }

        public Map<String, String> getErrorTags() {
            return tags.entrySet().stream()
                    .filter(e -> e.getKey().startsWith("error.") ||
                            "http.status_code".equals(e.getKey()) ||
                            "exception.class".equals(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        public String getTraceId() { return traceId; }
        public String getServiceName() { return serviceName; }
        public String getOperationName() { return operationName; }
        public long getStartTime() { return startTime; }
        public double getDuration() { return duration; }
        public Map<String, String> getTags() { return Collections.unmodifiableMap(tags); }
        public String getParentSpanId() { return parentSpanId; }
        public String getSpanId() { return spanId; }
    }
}