package com.grape.knowledgebase.srpingkafka.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageDto {
    // Üzenet, amit majd a Kafka felé küldünk el
    private String message;
    // Db szám, amennyiszer küldjük el az üzenetett
    private int count;
}
