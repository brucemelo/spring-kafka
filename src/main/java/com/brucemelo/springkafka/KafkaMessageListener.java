package com.brucemelo.springkafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;

import static com.brucemelo.springkafka.KafkaConfig.TOPIC_1;

@Configuration
public class KafkaMessageListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaMessageListener.class);

    @KafkaListener(id = "myId1", topics = TOPIC_1)
    public void listen1(String in) {
        log.info("listen1 = {}", in);
    }

    @KafkaListener(id = "myId2", topicPartitions = {@TopicPartition(topic = TOPIC_1, partitions = "0")})
    public void listen2(String in) {
        log.info("listen2 = {}", in);
    }

}
