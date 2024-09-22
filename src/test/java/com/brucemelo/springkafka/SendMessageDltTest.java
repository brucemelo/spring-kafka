package com.brucemelo.springkafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


@SpringBootTest
@Testcontainers
class SendMessageDltTest {

    private static final String TOPIC_2_DLT = "topic2.DLT";
    private static final String TOPIC_2 = "topic2";

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.10"));

    @Autowired
    SendMessageService sendMessageService;

    @Bean
    public NewTopic topic2() {
        return TopicBuilder.name(TOPIC_2)
                .partitions(3)
                .build();
    }

    private static KafkaConsumer<String, String> kafkaConsumer;

    @BeforeAll
    static void setup() {
        var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(List.of(TOPIC_2_DLT));
    }

    @AfterAll
    static void close() {
        kafkaConsumer.close();
    }

    @KafkaListener(topics = TOPIC_2, id = "idtopic2")
    public void listen2(String in) {
        throw new RuntimeException("Simulate failure");
    }

    @Test
    @DisplayName("Should have a message in DLT (Dead Letter Topic)")
    void testDlt() {
        sendMessageService.sendAsync("message1", TOPIC_2);
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, TOPIC_2_DLT, Duration.ofSeconds(3));
        Assertions.assertEquals(singleRecord.value(), "message1");
    }

}