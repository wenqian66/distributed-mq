package com.example.mq.controller;

import com.example.mq.cache.MessageCacheService;
import com.example.mq.entity.MessageState;
import com.example.mq.model.Order;
import com.example.mq.producer.OrderProducer;
import com.example.mq.repository.MessageRepository;
import io.micrometer.core.annotation.Timed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Operator-facing endpoints for the message queue.
 *
 * <p>The DLQ is intentionally not auto-replayed — that would just send broken
 * messages back to a consumer that already failed three times on them. Instead
 * an operator inspects {@code GET /admin/messages/failed}, fixes whatever was
 * wrong (a buggy consumer, a missing reference, a downstream outage), and
 * triggers {@code POST /admin/messages/{orderId}/replay}.
 *
 * <p>Production hardening checklist (left out for brevity):
 * <ul>
 *   <li>Authentication — these endpoints are dangerous</li>
 *   <li>Audit log of every replay with the operator's identity</li>
 *   <li>Rate limit so a script-happy operator can't replay 1M failures at once</li>
 * </ul>
 */
@RestController
@RequestMapping("/admin/messages")
@RequiredArgsConstructor
@Slf4j
public class MessageAdminController {

    private final MessageRepository messageRepository;
    private final MessageCacheService cacheService;
    private final OrderProducer orderProducer;

    @Timed(value = "message.lookup.latency", description = "Latency of message state lookups (cache-aside)")
    @GetMapping("/{orderId}")
    public ResponseEntity<MessageState> get(@PathVariable String orderId) {
        return cacheService.findByOrderId(orderId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Enumerate everything stuck in DLQ-land. */
    @GetMapping("/failed")
    public List<MessageState> listFailed() {
        return messageRepository.findByStatus(MessageState.Status.FAILED);
    }

    /**
     * Replay a previously-DLT'd message back onto its main topic.
     *
     * <p>We rebuild a synthetic {@link Order} from the persisted state. This is
     * lossless because we captured everything we need at consume time. If your
     * payload is more complex, store the original Kafka payload bytes verbatim
     * and republish those instead.
     */
    @PostMapping("/{orderId}/replay")
    public ResponseEntity<Map<String, Object>> replay(@PathVariable String orderId) {
        return messageRepository.findByOrderId(orderId)
                .map(state -> {
                    Order order = Order.builder()
                            .orderId(state.getOrderId())
                            .customerId(state.getCustomerId())
                            .amount(state.getAmount())
                            .priority(state.getPriority())
                            .createdAt(Instant.now())
                            .build();

                    orderProducer.send(order);

                    // Reset the row so we can tell the difference between
                    // "still failed" and "we replayed it" on subsequent inspection.
                    state.setStatus(MessageState.Status.PENDING);
                    state.setDltTopic(null);
                    cacheService.save(state);

                    log.info("Replayed order {} back to its priority topic", orderId);
                    return ResponseEntity.ok(Map.<String, Object>of(
                            "orderId", orderId,
                            "status", "replayed",
                            "previousAttempts", state.getAttempts()
                    ));
                })
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    /** Manually publish an order — useful for smoke-testing in dev. */
    @PostMapping
    public ResponseEntity<Map<String, String>> publish(@RequestBody Order order) {
        if (order.getCreatedAt() == null) {
            order.setCreatedAt(Instant.now());
        }
        if (order.getPriority() == null) {
            order.setPriority(Order.Priority.NORMAL);
        }
        orderProducer.send(order);
        return ResponseEntity.accepted().body(Map.of(
                "orderId", order.getOrderId(),
                "topic", "orders." + order.getPriority().name().toLowerCase()
        ));
    }
}
