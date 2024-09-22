package com.brucemelo.springkafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import static com.brucemelo.springkafka.KafkaConfig.TOPIC_1;

@RestController
@RequestMapping("/send")
public class SendMessageController {

    @Autowired
    SendMessageService sendMessageService;

    @PostMapping
    public void send(@RequestBody String message) {
        sendMessageService.send(message, TOPIC_1);
    }

    @PostMapping("/async")
    public void sendAsync(@RequestBody String message) {
        sendMessageService.sendAsync(message, TOPIC_1);
    }

}
