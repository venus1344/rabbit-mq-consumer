package com.rdxio;

import java.util.Base64;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class MetricsConsumer implements AutoCloseable {
    private final static String QUEUE_NAME = "metrics";
    private final static String ALGORITHM = "AES/CBC/PKCS5Padding";
    private final static String ENCRYPTION_KEY = Config.get("encryption.key");
    private final static String SECRET_KEY = Config.get("encryption.secret");
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MetricsWorker worker;

    static class EncryptedMessage {
        public String encryptedData;
        public String iv;
        public String hash;
    }

    public MetricsConsumer() throws Exception {
        this.worker = new MetricsWorker();
        new MetricsDashboard(worker, worker.getJedisPool());
    }

    private String decrypt(String encryptedData, String ivString) throws Exception {
        Cipher cipher = Cipher.getInstance(ALGORITHM);
        SecretKeySpec keySpec = new SecretKeySpec(ENCRYPTION_KEY.getBytes(), "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(Base64.getDecoder().decode(ivString));
        
        cipher.init(Cipher.DECRYPT_MODE, keySpec, ivSpec);
        byte[] decrypted = cipher.doFinal(Base64.getDecoder().decode(encryptedData));
        return new String(decrypted);
    }

    private String calculateHMAC(String data) {
        try {
            Mac sha256Hmac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secretKey = new SecretKeySpec(SECRET_KEY.getBytes(), "HmacSHA256");
            sha256Hmac.init(secretKey);
            return Base64.getEncoder().encodeToString(sha256Hmac.doFinal(data.getBytes()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate HMAC", e);
        }
    }
    // {cpuUsage=40.24%, usedMemory=10.42 GB, totalStorage=2.05 TB, memoryUsage=65.43%, totalMemory=15.92 GB, pcIdentifier=7X-DESKTOP-OON3NCL}
    // {cpuUsage=40.24%, usedMemory=10.42 GB, totalStorage=2.05 TB, memoryUsage=65.43%, totalMemory=15.92 GB, pcIdentifier=7X-DESKTOP-OON3NCL}
    // the above is the message format

    private void processMetrics(Map<String, Object> metrics) throws Exception {
        System.out.println("Processing metrics: " + metrics);
        String messageId = metrics.get("pcIdentifier") + ":" + System.currentTimeMillis();
        try {
            worker.processMetrics(messageId, metrics);
        } catch (Exception e) {
            System.err.println("Failed to process metrics: " + e.getMessage());
            throw e;
        }
    }

    private void consumeMessage(String messageJson) throws Exception {
        EncryptedMessage message = objectMapper.readValue(messageJson, EncryptedMessage.class);
        
        // Verify hash
        String calculatedHash = calculateHMAC(message.encryptedData);
        if (!calculatedHash.equals(message.hash)) {
            throw new SecurityException("Message integrity check failed");
        }
        
        // Decrypt
        String decryptedJson = decrypt(message.encryptedData, message.iv);
        Map<String, Object> metrics = objectMapper.readValue(decryptedJson, Map.class);
        processMetrics(metrics);
    }

    public void startConsuming() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUri(Config.get("rabbitmq.uri"));
        factory.setAutomaticRecoveryEnabled(true);        
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            
            boolean durable = true;
            channel.queueDeclare(QUEUE_NAME, durable, false, false, null);
            
            // Set QoS to process one message at a time
            channel.basicQos(1);
            
            System.out.println(" [*] Connected to RabbitMQ. Waiting for messages from metrics queue.");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String messageJson = new String(delivery.getBody(), "UTF-8");
                    System.out.println(" [x] Received encrypted message");
                    
                    consumeMessage(messageJson);
                    
                    // Manually acknowledge the message
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    System.out.println(" [x] Message processed and acknowledged");
                } catch (Exception e) {
                    System.err.println(" [!] Error processing message: " + e.getMessage());
                    e.printStackTrace();
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                }
            };

            // Set autoAck to false for manual acknowledgment
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
                System.out.println(" [!] Consumer cancelled by broker");
            });
            
            // Keep the main thread alive
            while (true) {
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (worker != null) {
            worker.close();
        }
    }
} 