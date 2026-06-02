package com.example.mq.producer;

import com.example.mq.model.Order;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes orders to one of three priority topics.
 *
 * <p>The interview talking point: Kafka does NOT support priority within a
 * single topic — you can't tell a consumer "process message X before message Y".
 * The standard pattern is to split into N topics by priority and have the
 * consumer poll them in order. That's what we do here.
 *
 * <p>The partition key is the {@code customerId} so all messages for the same
 * customer land in the same partition and are processed in order.
 */
@Service
@Slf4j
public class OrderProducer {

    public static final String TOPIC_HIGH = "orders.high";
    public static final String TOPIC_NORMAL = "orders.normal";
    public static final String TOPIC_LOW = "orders.low";

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final Map<Order.Priority, Counter> sentCounters;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate, MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.sentCounters = new EnumMap<>(Order.Priority.class);
        for (Order.Priority p : Order.Priority.values()) {
            sentCounters.put(p, Counter.builder("messages.sent")
                    .tag("priority", p.name())
                    .description("Messages sent to Kafka by priority")
                    .register(meterRegistry));
        }
    }

    public CompletableFuture<SendResult<String, Order>> send(Order order) {
        String topic = topicFor(order.getPriority());
        log.debug("Sending order {} to {}", order.getOrderId(), topic);
        sentCounters.get(order.getPriority()).increment();
        return kafkaTemplate.send(topic, order.getCustomerId(), order);
    }

    private static String topicFor(Order.Priority priority) {
        return switch (priority) {
            case HIGH -> TOPIC_HIGH;
            case NORMAL -> TOPIC_NORMAL;
            case LOW -> TOPIC_LOW;
        };
    }
}
