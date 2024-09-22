package com.brucemelo.springkafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConfig {

    public static final String TOPIC_1 = "topic1";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(ConsumerFactory<Object,Object> consumerFactory,
                                                                                                 KafkaTemplate<?, ?> template) {
        var factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        var recoverer = new DeadLetterPublishingRecoverer(template);
        var backOff = new FixedBackOff(1000L, 2L);
        var commonErrorHandler = new DefaultErrorHandler(recoverer, backOff);
        factory.setCommonErrorHandler(commonErrorHandler);
        return factory;
    }

    @Bean
    public NewTopic topic1() {
        return TopicBuilder.name(TOPIC_1)
                .partitions(5)
                .build();
    }

}
