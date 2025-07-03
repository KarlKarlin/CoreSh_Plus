package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;

@Service
public class JaegerTracesProcessing {
    private static final String REDIS_TRACE_PREFIX = "jaeger:trace:";
    private static final double MICRO_TO_MILLI = 1000.0;

    private final JaegerQueryService jaegerQueryService;
    private final ConfigProcessing configProcessing;
    private final RedisService redisService;

    public JaegerTracesProcessing(JaegerQueryService jaegerQueryService,
                                  ConfigProcessing configProcessing,
                                  RedisService redisService) {
        this.jaegerQueryService = jaegerQueryService;
        this.configProcessing = configProcessing;
        this.redisService = redisService;
    }

    public void fixErrorRate(String serviceName) throws IOException {
        int errorsCount = jaegerQueryService.getErrorTracesCount(serviceName, configProcessing.getErrorWindowHours());
        int allTracesCount = jaegerQueryService.getAllTracesForService(serviceName, configProcessing.getErrorWindowHours()).size();

        boolean isHighErrorRate = allTracesCount > 0 &&
                (errorsCount * 100.0 / allTracesCount) > configProcessing.getMaxErrorRate();

        if(!isHighErrorRate) {
            return;
        }

        JaegerQueryService.ErrorTraceInfo lastErrorDetails = jaegerQueryService.getLastErrorTraceForService(
                serviceName,
                configProcessing.getErrorWindowHours()
        );

        if (redisService.exists(REDIS_TRACE_PREFIX + "errorFix:" + lastErrorDetails.traceId())) {
            return;
        }

        redisService.setWithExpiry(
                REDIS_TRACE_PREFIX + "errorFix:" + lastErrorDetails.traceId(),
                "analyzed",
                configProcessing.getErrorWindowHours() * 60L
        );

        String message = String.format("""
        *Service:* %s
        *Error Count:* %d
        *Trace Id:* %s
        *Error Details:* %s
        *Similar Problem IDs:* %s
        """,
                serviceName,
                errorsCount,
                lastErrorDetails.traceId(),
                lastErrorDetails.errorDetails(),
                redisService.getProblemsByConditional("critical", 5)
        );

        redisService.commentProblem(serviceName, message, "critical");

        if(redisService.getServiceVersionHistory(serviceName).size() > 1) {

            if (!redisService.rollbackToPreviousVersion(serviceName)) return;

            String rollbackMessage = String.format("""
            *Rollback Executed:*
            *Service:* %s
            *Error Count:* %d
            *Trigger Trace:* %s
            *Similar Problem IDs:* %s
            """,
                    serviceName,
                    errorsCount,
                    lastErrorDetails.traceId(),
                    redisService.getProblemsByConditional("critical", 5)
            );
            redisService.commentProblem(serviceName, rollbackMessage, "critical");
        }
    }

    public void analyzeServiceTraces() {
        double maxLatencyThresholdMs = configProcessing.getMaxLatencyRate();
        List<String> allServices = configProcessing.getServicesForTracer();

        jaegerQueryService.getTraceIdsForWaterfall().forEach(traceId -> {
            if (!redisService.exists(REDIS_TRACE_PREFIX + traceId)) {
                TraceAnalysisResult analysis = analyzeCompleteTrace(traceId, allServices);
                if (analysis.hasProblems()) {
                    try {
                        notifyAboutProblematicTrace(analysis, maxLatencyThresholdMs);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                redisService.setWithExpiry(REDIS_TRACE_PREFIX + traceId, "analyzed", configProcessing.getErrorWindowHours() * 60L);
            }
        });
    }

    private TraceAnalysisResult analyzeCompleteTrace(String traceId, List<String> allServices) {
        TraceAnalysisResult result = new TraceAnalysisResult(traceId);
        allServices.forEach(service -> {
            jaegerQueryService.getTraceSpan(traceId, service).ifPresent(result::addSpan);
        });
        return result;
    }

    private void notifyAboutProblematicTrace(TraceAnalysisResult analysis, double maxLatencyThresholdMs) throws IOException {

        String message = String.format("""
            *Trace ID:* %s
            *Main Service:* %s
            *Total Duration:* %.2fms
            *Problems Detected:* %s
            *Request Flow:* %s
            *Similar Problem IDs:* %s
            """,
                analysis.getTraceId(),
                configProcessing.getMainJaegerService(),
                analysis.getTotalDuration(),
                buildProblemsList(analysis, maxLatencyThresholdMs),
                buildRequestGraph(analysis),
                redisService.getProblemsByConditional("critical", 5)
        );

        redisService.commentProblem("Unknown", message, "critical");

        if (analysis.hasCriticalErrors()) {
            fixErrorRate(configProcessing.getMainJaegerService());
        }
    }

    private class TraceAnalysisResult {
        private final String traceId;
        private final List<JaegerQueryService.TraceSpan> spans = new ArrayList<>();

        public TraceAnalysisResult(String traceId) {
            this.traceId = traceId;
        }

        public void addSpan(JaegerQueryService.TraceSpan span) {
            this.spans.add(span);
        }

        public boolean hasProblems() {
            return hasErrors() || hasHighLatency();
        }

        public boolean hasErrors() {
            return spans.stream().anyMatch(JaegerQueryService.TraceSpan::isError);
        }

        public boolean hasCriticalErrors() {
            return spans.stream()
                    .filter(JaegerQueryService.TraceSpan::isError)
                    .anyMatch(span -> {
                        String statusCode = span.getErrorTags().get("http.status_code");
                        return "500".equals(statusCode) ||
                                "501".equals(statusCode) ||
                                "502".equals(statusCode) ||
                                "503".equals(statusCode) ||
                                "504".equals(statusCode) ||
                                "505".equals(statusCode) ||

                                "database_error".equals(span.getErrorTags().get("error.type")) ||
                                "timeout".equals(span.getErrorTags().get("error.type")) ||
                                "connection_failed".equals(span.getErrorTags().get("error.type"));
                    });
        }

        public boolean hasHighLatency() {
            return spans.stream()
                    .anyMatch(span -> toMilliseconds((long) span.getDuration()) > configProcessing.getMaxLatencyRate());
        }

        public double getTotalDuration() {
            if (spans.isEmpty()) return 0;
            long start = spans.stream().mapToLong(JaegerQueryService.TraceSpan::getStartTime).min().orElse(0);
            long end = spans.stream()
                    .mapToLong(span -> span.getStartTime() + (long)span.getDuration())
                    .max().orElse(0);
            return toMilliseconds(end - start);
        }

        public String getTraceId() { return traceId; }
        public List<JaegerQueryService.TraceSpan> getSpans() { return Collections.unmodifiableList(spans); }
    }

    private String buildRequestGraph(TraceAnalysisResult analysis) {
        if (analysis.getSpans().isEmpty()) {
            return "No spans available for this trace";
        }

        StringBuilder graph = new StringBuilder();
        long startTime = analysis.getSpans().stream()
                .mapToLong(JaegerQueryService.TraceSpan::getStartTime)
                .min()
                .orElse(0);

        analysis.getSpans().stream()
                .sorted(Comparator.comparingLong(JaegerQueryService.TraceSpan::getStartTime))
                .forEach(span -> {
                    double offsetMs = toMilliseconds(span.getStartTime() - startTime);
                    double durationMs = toMilliseconds((long) span.getDuration());

                    graph.append(String.format("[+%.2fms] %s ", offsetMs, span.getServiceName()))
                            .append("▬".repeat((int) (durationMs / 10)));

                    if (span.isError()) {
                        graph.append(" ❌ ");
                        span.getErrorTags().forEach((k, v) ->
                                graph.append(k).append("=").append(v).append(" "));
                    } else if (durationMs > configProcessing.getMaxLatencyRate()) {
                        graph.append(String.format(" ⚠️ (%.2fms)", durationMs));
                    }
                    graph.append("\n");
                });

        return graph.toString();
    }

    private String buildProblemsList(TraceAnalysisResult analysis, double maxLatencyThresholdMs) {
        StringBuilder problems = new StringBuilder();

        analysis.getSpans().forEach(span -> {
            if (span.isError()) {
                problems.append(String.format(
                        "- %s: ERROR - %s%n",
                        span.getServiceName(),
                        span.getErrorTags().getOrDefault("error.message", "Unknown error")
                ));
            } else {
                double durationMs = toMilliseconds((long) span.getDuration());
                if (durationMs > maxLatencyThresholdMs) {
                    problems.append(String.format(
                            "- %s: High latency %.2fms (threshold: %.2fms)%n",
                            span.getServiceName(),
                            durationMs,
                            maxLatencyThresholdMs
                    ));
                }
            }
        });

        return problems.isEmpty() ?
                "No significant problems detected" :
                problems.toString();
    }

    private double toMilliseconds(long microseconds) {
        return microseconds / MICRO_TO_MILLI;
    }
}