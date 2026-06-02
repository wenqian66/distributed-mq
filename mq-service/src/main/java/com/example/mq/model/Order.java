package com.example.mq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * The payload sent over Kafka.
 *
 * <p>{@link Priority} drives which topic the producer publishes to.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    private String orderId;
    private String customerId;
    private BigDecimal amount;
    private Priority priority;
    private Instant createdAt;

    public enum Priority {
        HIGH,
        NORMAL,
        LOW;

        @JsonCreator
        public static Priority fromValue(@JsonProperty("priority") String value) {
            if (value == null) {
                return NORMAL;
            }
            return Priority.valueOf(value.toUpperCase());
        }
    }
}
