package com.example.mq.repository;

import com.example.mq.entity.MessageState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface MessageRepository extends JpaRepository<MessageState, Long> {

    Optional<MessageState> findByOrderId(String orderId);

    /** Used by the replay tool to enumerate failed messages. */
    List<MessageState> findByStatus(MessageState.Status status);
}
