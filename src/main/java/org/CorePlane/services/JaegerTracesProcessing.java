package org.CorePlane.services;

import org.CorePlane.configurations.ConfigProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class JaegerTracesProcessing {
    private static final Logger logger = LoggerFactory.getLogger(JaegerTracesProcessing.class);
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

        if (allTracesCount == 0) {
            logger.debug("No traces found for service {} in lookback window", serviceName);
            return;
        }

        double errorRate = (errorsCount * 100.0) / allTracesCount;
        boolean isHighErrorRate = errorRate > configProcessing.getMaxErrorRate();

        if (!isHighErrorRate) {
            logger.debug("Error rate {}% below threshold for service {}", errorRate, serviceName);
            return;
        }

        JaegerQueryService.ErrorTraceInfo lastErrorDetails = jaegerQueryService.getLastErrorTraceForService(
                serviceName,
                configProcessing.getErrorWindowHours()
        );

        if (lastErrorDetails.traceId() == null) {
            logger.warn("No error trace details found despite high error rate for service {}", serviceName);
            return;
        }

        if (redisService.exists(REDIS_TRACE_PREFIX + "errorFix:" + lastErrorDetails.traceId())) {
            logger.debug("Error for trace {} already processed", lastErrorDetails.traceId());
            return;
        }

        redisService.setWithExpiry(
                REDIS_TRACE_PREFIX + "errorFix:" + lastErrorDetails.traceId(),
                "analyzed",
                configProcessing.getErrorWindowHours() * 60L
        );

        String message = String.format("""
            *Service:* %s
            *Error Rate:* %.2f%%
            *Error Count:* %d
            *Total Traces:* %d
            *Trace Id:* %s
            *Error Details:* %s
            *Similar Problem IDs:* %s
            """,
                serviceName,
                errorRate,
                errorsCount,
                allTracesCount,
                lastErrorDetails.traceId(),
                lastErrorDetails.errorDetails(),
                redisService.getProblemsByConditional("critical", 5)
        );

        redisService.commentProblem(serviceName, message, "critical");

        if (redisService.getServiceVersionHistory(serviceName).size() > 1) {
            if (!redisService.rollbackToPreviousVersion(serviceName)) {
                logger.error("Failed to rollback service {}", serviceName);
                return;
            }

            String rollbackMessage = String.format("""
                *Rollback Executed:*
                *Service:* %s
                *Error Rate:* %.2f%%
                *Trigger Trace:* %s
                *Similar Problem IDs:* %s
                """,
                    serviceName,
                    errorRate,
                    lastErrorDetails.traceId(),
                    redisService.getProblemsByConditional("critical", 5)
            );
            redisService.commentProblem(serviceName, rollbackMessage, "critical");
        }
    }

    public void analyzeServiceTraces() {
        double maxLatencyThresholdMs = configProcessing.getMaxLatencyRate();
        List<String> allServices = configProcessing.getServicesForTracer();

        if (allServices.isEmpty()) {
            logger.warn("No services configured for tracing");
            return;
        }

        List<String> traceIds = jaegerQueryService.getTraceIdsForWaterfall();
        if (traceIds.isEmpty()) {
            logger.debug("No traces found for analysis");
            return;
        }

        traceIds.forEach(traceId -> {
            try {
                if (redisService.exists(REDIS_TRACE_PREFIX + traceId)) {
                    return;
                }

                List<JaegerQueryService.TraceSpan> completeTrace = jaegerQueryService.getCompleteTrace(traceId);
                if (completeTrace.isEmpty()) {
                    logger.debug("Empty trace received for ID: {}", traceId);
                    return;
                }

                List<JaegerQueryService.TraceSpan> relevantSpans = completeTrace.stream()
                        .filter(span -> allServices.contains(span.getServiceName()))
                        .collect(Collectors.toList());

                if (relevantSpans.isEmpty()) {
                    logger.debug("No relevant spans found in trace {}", traceId);
                    redisService.setWithExpiry(
                            REDIS_TRACE_PREFIX + traceId,
                            "no_relevant_spans",
                            configProcessing.getErrorWindowHours() * 60L
                    );
                    return;
                }

                TraceAnalysisResult analysis = analyzeCompleteTrace(relevantSpans);
                if (analysis.hasProblems()) {
                    notifyAboutProblematicTrace(analysis, maxLatencyThresholdMs);
                }

                redisService.setWithExpiry(
                        REDIS_TRACE_PREFIX + traceId,
                        "analyzed",
                        configProcessing.getErrorWindowHours() * 60L
                );
            } catch (IOException e) {
                logger.error("Failed to analyze trace {}", traceId, e);
                redisService.commentProblem("Unknown",
                        "Failed to analyze trace: " + traceId + " - " + e.getMessage(),
                        "notification");
            }
        });
    }

    private TraceAnalysisResult analyzeCompleteTrace(List<JaegerQueryService.TraceSpan> spans) {
        TraceAnalysisResult result = new TraceAnalysisResult(
                spans.isEmpty() ? "unknown" : spans.get(0).getTraceId()
        );
        spans.forEach(result::addSpan);
        return result;
    }

    private String buildRequestGraph(TraceAnalysisResult analysis) {
        if (analysis.getSpans().isEmpty()) {
            return "No spans available for this trace";
        }

        Map<String, JaegerQueryService.TraceSpan> spanMap = analysis.getSpans().stream()
                .collect(Collectors.toMap(JaegerQueryService.TraceSpan::getSpanId, Function.identity()));

        Map<String, List<JaegerQueryService.TraceSpan>> spanTree = analysis.getSpans().stream()
                .filter(span -> span.getParentSpanId() != null)
                .collect(Collectors.groupingBy(JaegerQueryService.TraceSpan::getParentSpanId));

        List<JaegerQueryService.TraceSpan> rootSpans = analysis.getSpans().stream()
                .filter(span -> !spanMap.containsKey(span.getParentSpanId()))
                .collect(Collectors.toList());

        if (rootSpans.isEmpty()) {
            return "Could not determine root spans for this trace";
        }

        StringBuilder graph = new StringBuilder();
        long startTime = rootSpans.stream()
                .mapToLong(JaegerQueryService.TraceSpan::getStartTime)
                .min()
                .orElse(0);

        rootSpans.forEach(span ->
                buildSpanGraph(graph, span, spanTree, spanMap, startTime, 0));

        return graph.toString();
    }

    private void buildSpanGraph(StringBuilder graph, JaegerQueryService.TraceSpan span,
                                Map<String, List<JaegerQueryService.TraceSpan>> spanTree,
                                Map<String, JaegerQueryService.TraceSpan> spanMap,
                                long traceStartTime, int depth) {
        double offsetMs = (span.getStartTime() - traceStartTime) / MICRO_TO_MILLI;
        double durationMs = span.getDuration() / MICRO_TO_MILLI;

        String indent = "  ".repeat(depth);
        graph.append(indent)
                .append(String.format("[+%.2fms] %s ", offsetMs, span.getServiceName()));

        int barLength = Math.max(1, (int) (durationMs / 5));
        String durationBar = "▰".repeat(barLength);

        if (span.isError()) {
            graph.append("🔥 ")
                    .append(durationBar)
                    .append(" ❌ ");
            span.getErrorTags().forEach((k,v) ->
                    graph.append(k).append("=").append(v).append(" "));
        } else if (durationMs > configProcessing.getMaxLatencyRate()) {
            graph.append("⚠️ ")
                    .append(durationBar)
                    .append(String.format(" (%.2fms)", durationMs));
        } else {
            graph.append("✅ ")
                    .append(durationBar)
                    .append(String.format(" (%.2fms) ✓", durationMs));
        }

        spanTree.getOrDefault(span.getSpanId(), Collections.emptyList())
                .forEach(child -> buildSpanGraph(
                        graph, child, spanTree, spanMap, traceStartTime, depth + 1));
    }

    private void notifyAboutProblematicTrace(TraceAnalysisResult analysis, double maxLatencyThresholdMs) throws IOException {
        String message = String.format("""
            *Trace ID:* %s
            *Main Service:* %s
            *Total Duration:* %.2fms
            *Problems Detected:* %s
            *Request Flow:* 
            %s
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
                double durationMs = span.getDuration() / MICRO_TO_MILLI;
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
                        return statusCode != null && statusCode.matches("5\\d\\d");
                    });
        }

        public boolean hasHighLatency() {
            return spans.stream()
                    .anyMatch(span -> (span.getDuration() / MICRO_TO_MILLI) > configProcessing.getMaxLatencyRate());
        }

        public double getTotalDuration() {
            if (spans.isEmpty()) return 0;

            long start = spans.stream()
                    .mapToLong(JaegerQueryService.TraceSpan::getStartTime)
                    .min()
                    .orElse(0);

            long end = spans.stream()
                    .mapToLong(span -> span.getStartTime() + (long)span.getDuration())
                    .max()
                    .orElse(0);

            return (end - start) / MICRO_TO_MILLI;
        }

        public String getTraceId() { return traceId; }
        public List<JaegerQueryService.TraceSpan> getSpans() { return Collections.unmodifiableList(spans); }
    }
}