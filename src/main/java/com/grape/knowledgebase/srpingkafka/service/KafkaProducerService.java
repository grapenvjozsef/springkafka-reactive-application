package com.grape.knowledgebase.srpingkafka.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class KafkaProducerService {

    private final String kafkaTopic;
    private final String bootstrapServers;
    private final KafkaSender<Integer, String> sender;
    private final SimpleDateFormat dateFormat;

    public KafkaProducerService(@Value("${kafka.bootstrap-servers}")final String bootstrapServers,
                                @Value("${kafka.topic}") final String kafkaTopic) {
        this.bootstrapServers = bootstrapServers;
        this.kafkaTopic = kafkaTopic;

        final Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        final SenderOptions<Integer, String> senderOptions = SenderOptions.create(props);
        this.sender = KafkaSender.create(senderOptions);
        this.dateFormat = new SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy");
    }

    /** Üzenet küldése */
    public void publish(final String message, final int count) {
        sender.send(Flux.range(1, count)
                .map(i -> SenderRecord.create(new ProducerRecord<>(kafkaTopic, i, message), i)))
                .doOnError(e -> log.error("Send failed", e))
                .subscribe(r -> {
                    final RecordMetadata metadata = r.recordMetadata();
                    log.info(String.format("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                            r.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            dateFormat.format(new Date(metadata.timestamp()))));
                });
    }

}
