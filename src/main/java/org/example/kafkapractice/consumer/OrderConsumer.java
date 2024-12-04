package org.example.kafkapractice.consumer;

import lombok.extern.slf4j.Slf4j;
import org.example.kafkapractice.model.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderConsumer {

    @KafkaListener(topics = "orders", groupId = "order-group")
    public void listen(@Payload OrderEvent orderEvent,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset) {
        // try {
        log.info("Received order: {}, partition: {}, offset: {}", orderEvent.getOrderId(), partition, offset);
        processOrder(orderEvent);
        // } catch (Exception e) {
        //     log.error("Error processing order: {}", orderEvent.getOrderId(), e);
        //    handleError(orderEvent, e);
        // }
        // 에러 발생 시 DeadLetterConsumer에서 재처리 시도
    }

    protected void processOrder(OrderEvent orderEvent) {
        // 주문 처리 로직
        log.info("Processing order: {}", orderEvent.getOrderId());
    }

    protected void handleError(OrderEvent orderEvent, Exception e) {
        // 에러 처리 로직
        log.error("Error processing order: {}", orderEvent.getOrderId(), e);
    }
}
