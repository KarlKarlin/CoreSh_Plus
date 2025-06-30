package org.CorePlane.controllers;

import org.CorePlane.services.DockerSwarmService;
import org.CorePlane.services.RedisService;
import org.CorePlane.services.WriteStateService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.*;

@RestController
@RequestMapping("/api/problems")
public class CommentingController {

    private final RedisService redisService;
    private final WriteStateService writeStateService;
    private final DockerSwarmService dockerSwarmService;

    @Autowired
    public CommentingController(RedisService redisService, WriteStateService writeStateService, DockerSwarmService dockerSwarmService) {
        this.redisService = redisService;
        this.writeStateService = writeStateService;
        this.dockerSwarmService = dockerSwarmService;
    }

    @PostMapping("/{uuid}/comments")
    public ResponseEntity<?> addComment(
            @PathVariable String uuid,
            @RequestBody Map<String, String> request) {
        try {
            redisService.addCommentToProblem(uuid, request.get("comment"));
            return ResponseEntity.ok(Map.of("message", "Comment added successfully"));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{uuid}")
    public ResponseEntity<?> getProblem(@PathVariable String uuid) {
        try {
            String problem = redisService.getProblem(uuid);
            return ResponseEntity.ok(Map.of("description", problem));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{uuid}/comments")
    public ResponseEntity<?> getComments(@PathVariable String uuid) {
        try {
            List<String> comments = redisService.getComments(uuid);
            return ResponseEntity.ok(comments);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/by-conditional/{conditional}")
    public ResponseEntity<List<String>> getProblemsByConditional(
            @PathVariable String conditional,
            @RequestParam int limit) {
        return ResponseEntity.ok(redisService.getProblemsByConditional(conditional, limit));
    }

    @GetMapping("/keys")
    public ResponseEntity<List<String>> getAllProblemKeys() {
        return ResponseEntity.ok(redisService.getAllProblemKeys());
    }

    @GetMapping("/conditional-stats")
    public ResponseEntity<Map<String, Long>> getConditionalStatistics() {
        return ResponseEntity.ok(redisService.getConditionalStatistics());
    }

    @GetMapping("/service-conditional-stats")
    public ResponseEntity<Map<String, Long>> getServiceConditionalStatistics() {
        return ResponseEntity.ok(redisService.getConditionalStatisticsByService());
    }

    @GetMapping("/{uuid}/metadata")
    public ResponseEntity<?> getProblemMetadata(@PathVariable String uuid) {
        try {
            Map<String, String> metadata = redisService.getProblemMetadata(uuid);
            return ResponseEntity.ok(metadata);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/recent")
    public ResponseEntity<List<Map<String, String>>> getRecentProblems(
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(redisService.getRecentProblems(limit));
    }

    @GetMapping("/total-comments")
    public ResponseEntity<Long> getTotalCommentCount() {
        return ResponseEntity.ok(redisService.getTotalCommentCount());
    }

    @GetMapping("/all-with-metadata")
    public ResponseEntity<List<Map<String, String>>> getAllProblemsWithMetadata() {
        return ResponseEntity.ok(redisService.getAllProblemsWithMetadata());
    }

    @GetMapping("/get-states-per-service")
    public ResponseEntity<List<Map<String, String>>> getStatesPerService() {
        return ResponseEntity.ok(writeStateService.getServiceStatesWithPredictions());
    }

    @GetMapping("/get-recent-logs-for-problem/{service}/{timestampMillis}")
    public ResponseEntity<List<String>> getRecentLogsForProblem(
            @PathVariable String service,
            @PathVariable long timestampMillis) {
        if (dockerSwarmService == null) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonList("Service unavailable"));
        }
        try {
            List<String> logs = dockerSwarmService.getServiceLogsInTimeWindow(service, timestampMillis, 3);
            return ResponseEntity.ok(logs);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Collections.singletonList("Error retrieving logs: " + e.getMessage()));
        }
    }
}