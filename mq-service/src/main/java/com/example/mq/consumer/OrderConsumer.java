package com.example.mq.consumer;

import com.example.mq.cache.MessageCacheService;
import com.example.mq.entity.MessageState;
import com.example.mq.model.Order;
import com.example.mq.producer.OrderProducer;
import com.example.mq.repository.MessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC;
import static org.springframework.kafka.support.KafkaHeaders.RECEIVED_PARTITION;
import static org.springframework.kafka.support.KafkaHeaders.OFFSET;

/**
 * Consumes from all three priority topics and runs them through a non-blocking
 * retry chain backed by automatically-created retry topics.
 *
 * <h3>How {@code @RetryableTopic} works</h3>
 * <p>Spring Kafka transparently creates and manages four extra topics:
 * <pre>
 *   orders.high            (main)
 *   orders.high-retry-0    (10s backoff)
 *   orders.high-retry-1    (30s backoff)
 *   orders.high-retry-2    (2m backoff)
 *   orders.high-dlt        (dead letter)
 * </pre>
 * (One set per main topic.) When this method throws, the framework republishes
 * the message to the appropriate retry topic with the right delay header,
 * and a separate consumer (also managed by Spring Kafka) waits for the delay
 * before delivering the message back to this method.
 *
 * <p>Why this is the correct pattern: a {@code Thread.sleep(10s)} inside this
 * method would block the entire partition. Republishing means the partition's
 * read offset advances and other messages keep flowing.
 */
@Component
@Slf4j
public class OrderConsumer {

    private final MessageRepository messageRepository;
    private final MessageCacheService cacheService;
    private final Counter processedCounter;
    private final Counter failedCounter;

    public OrderConsumer(MessageRepository messageRepository,
                         MessageCacheService cacheService,
                         MeterRegistry meterRegistry) {
        this.messageRepository = messageRepository;
        this.cacheService = cacheService;
        this.processedCounter = Counter.builder("messages.processed")
                .description("Messages successfully processed")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("messages.failed")
                .description("Messages that exhausted retries and landed in DLT")
                .register(meterRegistry);
    }

    @RetryableTopic(
            attempts = "4",                                                   // 1 initial + 3 retries
            backoff = @Backoff(delay = 10_000, multiplier = 3.0),             // 10s -> 30s -> 90s
            autoCreateTopics = "true",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            // Spring Kafka resolves the DLT topic name as <main>-dlt by default; that's fine.
            // Don't retry on validation errors — those will never succeed.
            exclude = { IllegalArgumentException.class }
    )
    @KafkaListener(
            topics = { OrderProducer.TOPIC_HIGH, OrderProducer.TOPIC_NORMAL, OrderProducer.TOPIC_LOW },
            groupId = "${spring.kafka.consumer.group-id}",
            concurrency = "${kafka.consumer.concurrency:3}"
    )
    @Transactional
    public void consume(
            Order order,
            @Header(RECEIVED_TOPIC) String topic,
            @Header(RECEIVED_PARTITION) int partition,
            @Header(OFFSET) long offset
    ) {
        log.info("Processing order {} from {}-{}@{}", order.getOrderId(), topic, partition, offset);

        MessageState state = messageRepository
                .findByOrderId(order.getOrderId())
                .orElseGet(() -> MessageState.newPending(order));
        state.setLastSeenAt(Instant.now());
        state.setAttempts(state.getAttempts() + 1);
        cacheService.save(state);

        processOrder(order);

        state.markCompleted();
        cacheService.save(state);
        processedCounter.increment();
    }

    /**
     * The terminal handler for messages that exhausted all retries.
     *
     * <p>This runs on {@code orders.high-dlt}, {@code orders.normal-dlt}, etc.
     * It does NOT auto-replay — that's the whole point of a DLQ. We mark the
     * row in PostgreSQL as {@code FAILED} so an operator can investigate and
     * trigger a manual replay through {@link com.example.mq.controller.MessageAdminController}.
     */
    @DltHandler
    public void onDeadLetter(
            Order order,
            @Header(RECEIVED_TOPIC) String dltTopic
    ) {
        log.error("Order {} landed in DLT {} after exhausting retries", order.getOrderId(), dltTopic);

        messageRepository.findByOrderId(order.getOrderId()).ifPresent(state -> {
            state.markFailed(dltTopic);
            cacheService.save(state);
        });
        failedCounter.increment();
    }

    /** Stand-in for whatever business work this consumer actually does. */
    private void processOrder(Order order) {
        // Demo: throw on amounts above a threshold to exercise the retry/DLT path.
        if (order.getAmount() != null && order.getAmount().signum() < 0) {
            throw new IllegalStateException("Negative amount on order " + order.getOrderId());
        }
        // ...real fulfilment logic...
    }
}
