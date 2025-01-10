package com.rdxio;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class MetricsWorker implements AutoCloseable {
    private static final String POSTGRES_URL = Config.get("postgres.url");
    private static final String POSTGRES_USER = Config.get("postgres.user");
    private static final String POSTGRES_PASSWORD = Config.get("postgres.password");
    private static final long MEMORY_THRESHOLD = Long.parseLong(Config.get("memory.threshold"));
    private static final long MESSAGE_RETENTION_SECONDS = 120;
    
    private final JedisPool jedisPool;
    private final Connection dbConnection;
    private final ScheduledExecutorService scheduler;

    public MetricsWorker() throws Exception {
        this.jedisPool = new JedisPool(Config.get("redis.host"), 
            Integer.parseInt(Config.get("redis.port")));
        this.dbConnection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
        this.scheduler = Executors.newScheduledThreadPool(1);
        ensureTableExists();
        startCleanupScheduler();
    }

    private void ensureTableExists() throws Exception {
        if (!tableExists("device_metrics")) {
            System.out.println("Creating device_metrics table...");
            createTable();
            System.out.println("Table created successfully.");
        }
    }

    private boolean tableExists(String tableName) throws Exception {
        DatabaseMetaData meta = dbConnection.getMetaData();
        ResultSet tables = meta.getTables(null, null, tableName.toLowerCase(), new String[] {"TABLE"});
        return tables.next();
    }

    private void createTable() throws Exception {
        try (Statement stmt = dbConnection.createStatement()) {
            String sql = "CREATE TABLE device_metrics (" +
                "id SERIAL PRIMARY KEY," +
                "device_id VARCHAR(255) NOT NULL," +
                "cpu_usage DOUBLE PRECISION NOT NULL," +
                "memory_used DOUBLE PRECISION NOT NULL," +
                "memory_total DOUBLE PRECISION NOT NULL," +
                "mem_risk BOOLEAN NOT NULL," +
                "created_at TIMESTAMP NOT NULL" +
                ");" +
                "CREATE INDEX idx_device_metrics_device_id ON device_metrics(device_id);" +
                "CREATE INDEX idx_device_metrics_created_at ON device_metrics(created_at);";
            
            stmt.execute(sql);
        }
    }

    public void processMetrics(String messageId, Map<String, Object> metrics) throws Exception {
        try {
            boolean memRisk = calculateMemoryRisk(metrics);
            insertMetrics(metrics, memRisk);
            
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.lpush("process_metrics", messageId);
                jedis.hset("metrics:status", messageId, "queued");
                processQueuedMessage(messageId, jedis);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private void processQueuedMessage(String messageId, Jedis jedis) {
        try {
            // Process the message (we've already inserted into PostgreSQL)
            // Update status to processed
            jedis.hset("metrics:status", messageId, "processed");
            // Remove from queue
            jedis.lrem("process_metrics", 1, messageId);
        } catch (Exception e) {
            System.err.println("Error processing queued message: " + e.getMessage());
            markAsFailed(messageId);
        }
    }

    private void markAsFailed(String messageId) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset("metrics:status", messageId, "failed");
            // Keep failed messages in queue for retry
        } catch (Exception e) {
            System.err.println("Error marking message as failed: " + e.getMessage());
        }
    }

    private boolean calculateMemoryRisk(Map<String, Object> metrics) {
        try {
            System.out.println("Calculating memory risk: " + metrics);
            String usedMemory = metrics.get("usedMemory").toString().replace(" GB", "");
            double memoryGB = Double.parseDouble(usedMemory);
            return memoryGB > MEMORY_THRESHOLD; // Compare in GB
        } catch (Exception e) {
            System.err.println("Error calculating memory risk: " + e.getMessage());
            return false;
        }
    }

    private void insertMetrics(Map<String, Object> metrics, boolean memRisk) throws Exception {
        String sql = "INSERT INTO device_metrics (device_id, cpu_usage, memory_used, memory_total, mem_risk, created_at) " +
                    "VALUES (?, ?, ?, ?, ?, NOW())";
        
        try (PreparedStatement stmt = dbConnection.prepareStatement(sql)) {
            String cpuUsage = metrics.get("cpuUsage").toString().replace("%", "");
            String usedMemory = metrics.get("usedMemory").toString().replace(" GB", "");
            String totalMemory = metrics.get("totalMemory").toString().replace(" GB", "");
            
            stmt.setString(1, metrics.get("pcIdentifier").toString());
            stmt.setDouble(2, Double.parseDouble(cpuUsage));
            stmt.setDouble(3, Double.parseDouble(usedMemory));
            stmt.setDouble(4, Double.parseDouble(totalMemory));
            stmt.setBoolean(5, memRisk);
            
            stmt.executeUpdate();
        }
    }

    private void startCleanupScheduler() {
        // Run cleanup every 30 seconds
        scheduler.scheduleAtFixedRate(() -> {
            try {
                cleanupOldMessages(TimeUnit.SECONDS.toMillis(MESSAGE_RETENTION_SECONDS));
            } catch (Exception e) {
                System.err.println("Error in cleanup scheduler: " + e.getMessage());
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        try {
            scheduler.shutdown();
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("Error shutting down scheduler: " + e.getMessage());
        } finally {
            try {
                dbConnection.close();
                jedisPool.close();
            } catch (Exception e) {
                System.err.println("Error closing connections: " + e.getMessage());
            }
        }
    }

    public void retryFailedMessages() {
        try (Jedis jedis = jedisPool.getResource()) {
            // Get all messages in the queue
            List<String> queuedMessages = jedis.lrange("process_metrics", 0, -1);
            
            for (String messageId : queuedMessages) {
                String status = jedis.hget("metrics:status", messageId);
                if ("failed".equals(status)) {
                    System.out.println("Retrying failed message: " + messageId);
                    processQueuedMessage(messageId, jedis);
                }
            }
        } catch (Exception e) {
            System.err.println("Error retrying failed messages: " + e.getMessage());
        }
    }

    public void cleanupOldMessages(long retentionPeriodMillis) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Get all messages in status hash
            Map<String, String> allStatuses = jedis.hgetAll("metrics:status");
            long now = System.currentTimeMillis();
            
            for (Map.Entry<String, String> entry : allStatuses.entrySet()) {
                String messageId = entry.getKey();
                String status = entry.getValue();
                
                // Extract timestamp from messageId (assuming format: deviceId:timestamp)
                long messageTime = Long.parseLong(messageId.split(":")[1]);
                
                if ("processed".equals(status) && (now - messageTime > retentionPeriodMillis)) {
                    // Remove old processed messages
                    jedis.hdel("metrics:status", messageId);
                    jedis.lrem("process_metrics", 0, messageId);
                }
            }
        } catch (Exception e) {
            System.err.println("Error cleaning up old messages: " + e.getMessage());
        }
    }

    public JedisPool getJedisPool() {
        return jedisPool;
    }
} 