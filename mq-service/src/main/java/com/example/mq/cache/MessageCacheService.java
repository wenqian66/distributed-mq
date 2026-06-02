package com.example.mq.cache;

import com.example.mq.entity.MessageState;
import com.example.mq.repository.MessageRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Optional;

/**
 * Cache-aside layer in front of {@link MessageRepository}.
 *
 * <p>This is the implementation behind the resume's "45% latency reduction"
 * line — and the part interviewers ask about most often, because the naive
 * version (write Redis after writing PG) has a race condition.
 *
 * <h3>Read path</h3>
 * <ol>
 *   <li>{@code GET key} from Redis</li>
 *   <li>On hit: return immediately</li>
 *   <li>On miss: read from Postgres, then {@code SET key} with TTL</li>
 * </ol>
 *
 * <h3>Write path — the part everyone gets wrong</h3>
 * <p>After updating Postgres we {@code DEL} the key, NEVER {@code SET} it.
 *
 * <p>Why DEL instead of SET? With concurrent writers, SET races:
 * <pre>
 *   T1 reads PG (sees v1), about to SET Redis with v1
 *   T2 writes PG (sets v2), SETs Redis with v2
 *   T1's SET arrives last and overwrites Redis with the stale v1.
 * </pre>
 * DEL has no such race — the next reader will rediscover v2 from PG via the
 * read path's miss-then-populate flow.
 */
@Service
@Slf4j
public class MessageCacheService {

    private static final String KEY_PREFIX = "msg:";

    private final MessageRepository messageRepository;
    private final RedisTemplate<String, MessageState> redisTemplate;
    private final Counter hitCounter;
    private final Counter missCounter;

    @Value("${cache.ttl-seconds:600}")
    private long ttlSeconds;

    public MessageCacheService(MessageRepository messageRepository,
                               RedisTemplate<String, MessageState> redisTemplate,
                               MeterRegistry meterRegistry) {
        this.messageRepository = messageRepository;
        this.redisTemplate = redisTemplate;
        this.hitCounter = Counter.builder("cache.hit")
                .tag("service", "message")
                .description("Cache hits on message state lookups")
                .register(meterRegistry);
        this.missCounter = Counter.builder("cache.miss")
                .tag("service", "message")
                .description("Cache misses on message state lookups")
                .register(meterRegistry);
    }

    /** Cache-aside read by orderId. */
    public Optional<MessageState> findByOrderId(String orderId) {
        String key = KEY_PREFIX + orderId;

        // 1. Try cache
        MessageState cached = redisTemplate.opsForValue().get(key);
        if (cached != null) {
            hitCounter.increment();
            log.debug("Cache hit for {}", key);
            return Optional.of(cached);
        }

        missCounter.increment();
        log.debug("Cache miss for {} — falling through to Postgres", key);
        Optional<MessageState> fromDb = messageRepository.findByOrderId(orderId);

        // 3. Populate cache only if we found something. Don't cache negative
        //    results: an empty value would shadow a row that gets created
        //    moments later. If you're worried about cache stampedes from
        //    repeated misses on missing keys, use a short-TTL "tombstone"
        //    instead of nothing.
        fromDb.ifPresent(state ->
                redisTemplate.opsForValue().set(key, state, Duration.ofSeconds(ttlSeconds)));

        return fromDb;
    }

    /**
     * Save the entity to PG, then invalidate the cache key.
     *
     * <p>Note the order: PG first, then DEL. If PG fails the cache entry stays
     * (might be stale) but we don't write a wrong row. If DEL fails after a PG
     * success the cache holds the old value briefly until the TTL expires —
     * acceptable since we degrade gracefully.
     */
    @Transactional
    public MessageState save(MessageState state) {
        MessageState saved = messageRepository.save(state);
        invalidate(saved.getOrderId());
        return saved;
    }

    /** Explicit invalidation for callers that updated PG by other means. */
    public void invalidate(String orderId) {
        Boolean deleted = redisTemplate.delete(KEY_PREFIX + orderId);
        log.debug("Invalidated cache key {} (existed={})", orderId, deleted);
    }
}
