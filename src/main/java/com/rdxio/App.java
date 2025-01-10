package com.rdxio;

public class App {
    public static void main(String[] args) {
        MetricsConsumer consumer;
        try {
            consumer = new MetricsConsumer();
            
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down gracefully...");
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error during shutdown: " + e.getMessage());
                }
            }));
            
            consumer.startConsuming();
        } catch (Exception e) {
            System.err.println("Error starting consumer: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
