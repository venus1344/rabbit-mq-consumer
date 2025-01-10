package com.rdxio;

import static spark.Spark.*;
import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import java.util.*;

public class MetricsDashboard {
    private final JedisPool jedisPool;
    private final Gson gson = new Gson();
    private final MetricsWorker worker;

    public MetricsDashboard(MetricsWorker worker, JedisPool jedisPool) {
        this.worker = worker;
        this.jedisPool = jedisPool;
        setupRoutes();
    }

    private void setupRoutes() {
        port(8080);

        get("/dashboard", (req, res) -> {
            Map<String, Object> model = new HashMap<>();
            try (Jedis jedis = jedisPool.getResource()) {
                Map<String, String> statuses = jedis.hgetAll("metrics:status");
                List<Map<String, String>> failedJobs = new ArrayList<>();
                
                statuses.forEach((messageId, status) -> {
                    if ("failed".equals(status)) {
                        Map<String, String> job = new HashMap<>();
                        job.put("id", messageId);
                        job.put("status", status);
                        failedJobs.add(job);
                    }
                });
                
                model.put("failedJobs", failedJobs);
            }
            res.type("application/json");
            return gson.toJson(model);
        });

        post("/retry/:messageId", (req, res) -> {
            String messageId = req.params(":messageId");
            worker.retryFailedMessages();
            res.type("application/json");
            return gson.toJson(Map.of("status", "retry initiated"));
        });

        post("/retry-all", (req, res) -> {
            worker.retryFailedMessages();
            res.type("application/json");
            return gson.toJson(Map.of("status", "all retries initiated"));
        });
    }
} 