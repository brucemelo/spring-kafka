package com.brucemelo.springkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Service
public class SendMessageService {

    private static final Logger log = LoggerFactory.getLogger(SendMessageService.class);

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    public void send(String message, String topic) {
        try {
            SendResult<String, String> result = kafkaTemplate.send(topic, message).get();
            log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Failed to send message=[{}] due to: ", message, e);
        }
    }

    public void sendAsync(String message, String topic) {
        CompletableFuture<SendResult<String, String>> send = kafkaTemplate.send(topic, message);
        send.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            } else {
                log.error("Failed to send message=[{}] due to: ", message, ex);
            }
        });
    }

}
