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
class SendMessageServiceTest {

    private static final String TOPIC_3 = "topic3";
    private static final String TOPIC_4 = "topic4";
    private static final Duration TIMEOUT = Duration.ofSeconds(3);
    private static KafkaConsumer<String, String> kafkaConsumer;

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.10"));

    @Autowired
    SendMessageService sendMessageService;

    @Bean
    public NewTopic topic3() {
        return TopicBuilder.name(TOPIC_3)
                .partitions(5)
                .build();
    }

    @Bean
    public NewTopic topic4() {
        return TopicBuilder.name(TOPIC_4)
                .partitions(5)
                .build();
    }

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
        kafkaConsumer.subscribe(List.of(TOPIC_3, TOPIC_4));
    }

    @AfterAll
    static void close() {
        kafkaConsumer.close();
    }

    @Test
    @DisplayName("Should send async message to topic 3")
    void testMessageSentToTopic3() {
        sendMessageService.sendAsync("message3", TOPIC_3);
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, TOPIC_3, TIMEOUT);
        Assertions.assertEquals(singleRecord.value(), "message3");
    }

    @Test
    @DisplayName("Should send sync message to topic 4")
    void testMessageSentTopic4() {
        sendMessageService.send("message4", TOPIC_4);
        ConsumerRecord<String, String> singleRecord = KafkaTestUtils.getSingleRecord(kafkaConsumer, TOPIC_4, TIMEOUT);
        Assertions.assertEquals(singleRecord.value(), "message4");
    }

}