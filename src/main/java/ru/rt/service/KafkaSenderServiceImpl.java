package ru.rt.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Date;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaSenderServiceImpl implements KafkaSenderService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${message.topic.name}")
    private String topicName;

    @Override
    public void sendMessage(String msg) {
        log.info("sendMessage(): topicName='{}', msg='{}'", topicName, msg);
        kafkaTemplate.send(topicName, msg);
    }

    @Scheduled(initialDelay = 10000, fixedDelay = 5000)
    public void produce() {
        String msg = String.format("Current time: %s", new Date());
        sendMessage(msg);
    }
}
