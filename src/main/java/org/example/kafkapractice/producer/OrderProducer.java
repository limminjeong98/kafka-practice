package org.example.kafkapractice.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.example.kafkapractice.model.OrderEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderProducer {

    private static final String TOPIC = "orders";
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void sendOrder(OrderEvent orderEvent) {
        kafkaTemplate.send(TOPIC, orderEvent.getOrderId(), orderEvent)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message: {}", orderEvent.getOrderId(), ex);
                    } else {
                        log.info("Message sent successfully: {}, partition: {}", orderEvent.getOrderId(), result.getRecordMetadata().partition());
                    }
                });
    }

    public void sendOrderSync(OrderEvent orderEvent) throws Exception {
        try {
            SendResult<String, OrderEvent> result = kafkaTemplate.send(TOPIC, orderEvent.getOrderId(), orderEvent).get();
            log.info("Message sent synchronously: {}, partition: {}", orderEvent.getOrderId(), result.getRecordMetadata().partition());
        } catch (Exception e) {
            log.error("Error sending message synchronously", e);
            throw e;
        }
    }
}
