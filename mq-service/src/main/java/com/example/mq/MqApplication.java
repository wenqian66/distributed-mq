package com.example.mq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Entry point for the distributed message-queue service.
 *
 * <p>{@code @EnableKafka} turns on the {@code @KafkaListener} annotation
 * processing — this includes both regular listeners and the
 * {@code @RetryableTopic}-driven retry chain in {@link com.example.mq.consumer.OrderConsumer}.
 */
@SpringBootApplication
@EnableKafka
public class MqApplication {
    public static void main(String[] args) {
        SpringApplication.run(MqApplication.class, args);
    }
}
