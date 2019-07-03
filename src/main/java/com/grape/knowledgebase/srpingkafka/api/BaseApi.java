package com.grape.knowledgebase.srpingkafka.api;

import com.grape.knowledgebase.srpingkafka.entity.MessageDto;
import com.grape.knowledgebase.srpingkafka.service.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
@Slf4j
@RestController
@RequestMapping("/api/v1")
public class BaseApi {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/publish")
    public void publishToKafka(@RequestBody final MessageDto messageDto) {
        kafkaProducerService.publish(messageDto.getMessage(), messageDto.getCount());
    }
}
