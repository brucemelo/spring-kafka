package com.brucemelo.springkafka;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import static com.brucemelo.springkafka.KafkaConfig.TOPIC_1;

@Configuration
public class KafkaInitProducer {

    @Bean
    public ApplicationRunner runner(KafkaTemplate<String, String> template) {
        return args -> {
            template.send(TOPIC_1, 0, "key0", "test0");
            template.send(TOPIC_1, 0, "key1", "test1");
            template.send(TOPIC_1, "test2");
        };
    }

}
