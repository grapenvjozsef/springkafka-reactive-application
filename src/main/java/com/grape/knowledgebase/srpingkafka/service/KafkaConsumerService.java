package com.grape.knowledgebase.srpingkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class KafkaConsumerService {

    private final String kafkaTopic;
    private final String bootstrapServers;
    private final ReceiverOptions<Integer, String> receiverOptions;
    private final SimpleDateFormat dateFormat;

    public KafkaConsumerService(@Value("${kafka.bootstrap-servers}")final String bootstrapServers,
                                @Value("${kafka.topic}") final String kafkaTopic) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaTopic = kafkaTopic;

        final Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.receiverOptions = ReceiverOptions.create(props);
        this.dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");

        consumeMessages();
    }

    /** Elküldött üzenet kiolvasása és visszigazolása */
    public Disposable consumeMessages() {
        final ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(kafkaTopic))
                .addAssignListener(partitions -> log.info("onPartitionsAssigned []", partitions))
                .addRevokeListener(partitions -> log.info("onPartitionsRevoked {}", partitions));
        final Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
        return kafkaFlux.subscribe(record -> {
            final ReceiverOffset offset = record.receiverOffset();
            log.info(String.format("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                    offset.topicPartition(),
                    offset.offset(),
                    dateFormat.format(new Date(record.timestamp())),
                    record.key(),
                    record.value()));
            offset.acknowledge();
        });
    }


}
