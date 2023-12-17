package br.com.eduardomelle.mainconsumer.publisher;

import br.com.eduardomelle.mainconsumer.dto.MyDTO;
import br.com.eduardomelle.mainconsumer.enums.EventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class KafkaPublisher {

    @Value(value = "${kafka.topic}")
    private String topic;

    private final ObjectMapper objectMapper;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaPublisher(ObjectMapper objectMapper, KafkaTemplate<String, String> kafkaTemplate) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(MyDTO dto) {
        try {
            String message = this.objectMapper.writeValueAsString(dto);

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(this.topic, dto.getEventType().name(), message);

            getHeaders(dto.getEventType()).forEach((k, v) -> {
                producerRecord.headers().add(k, v.getBytes(StandardCharsets.UTF_16));
            });

            this.kafkaTemplate.send(producerRecord);

            log.info("Message sent {}", dto);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage());
        }
    }

    private Map<String, String> getHeaders(EventType eventType) {
        Map<String, String> headers = new HashMap<>();
        headers.put("eventType", eventType.getEvent());
        return headers;
    }

}
