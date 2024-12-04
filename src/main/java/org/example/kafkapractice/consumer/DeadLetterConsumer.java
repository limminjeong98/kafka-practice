package org.example.kafkapractice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.example.kafkapractice.model.OrderEvent;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeadLetterConsumer {

    @KafkaListener(topics = "orders.DLT", groupId = "dlt-group")
    public void listenDLT(@Payload OrderEvent orderEvent, Exception exception) {
        log.error("Received failed order in DLT: {}, Error: {}", orderEvent.getOrderId(), exception.getMessage());

    }
}
