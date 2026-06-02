package com.example.mq.entity;

import com.example.mq.model.Order;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * Audit + state record for every order that flows through the queue.
 *
 * <p>One row per orderId. The same row is updated on each retry, so
 * {@link #attempts} tells you how many times the consumer has tried.
 *
 * <p>Cached in Redis via the cache-aside pattern in
 * {@link com.example.mq.cache.MessageCacheService}.
 */
@Entity
@Table(
    name = "message_state",
    indexes = {
        @Index(name = "idx_msg_order_id", columnList = "orderId", unique = true),
        @Index(name = "idx_msg_status", columnList = "status")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageState implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true, length = 64)
    private String orderId;

    @Column(length = 64)
    private String customerId;

    @Column(precision = 18, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Order.Priority priority;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Status status;

    @Column(nullable = false)
    private int attempts;

    /** When set, the topic that DLT'd this message — useful for the replay tool. */
    @Column(length = 128)
    private String dltTopic;

    @Column(nullable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant lastSeenAt;

    public enum Status { PENDING, COMPLETED, FAILED }

    public static MessageState newPending(Order order) {
        Instant now = Instant.now();
        return MessageState.builder()
                .orderId(order.getOrderId())
                .customerId(order.getCustomerId())
                .amount(order.getAmount())
                .priority(order.getPriority())
                .status(Status.PENDING)
                .attempts(0)
                .createdAt(now)
                .lastSeenAt(now)
                .build();
    }

    public void markCompleted() {
        this.status = Status.COMPLETED;
        this.lastSeenAt = Instant.now();
    }

    public void markFailed(String dltTopic) {
        this.status = Status.FAILED;
        this.dltTopic = dltTopic;
        this.lastSeenAt = Instant.now();
    }
}
