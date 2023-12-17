package br.com.eduardomelle.mainconsumer.listener;

import br.com.eduardomelle.mainconsumer.dto.MyDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaMainListener {

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, MyDTO> consumerRecord) {
        log.info("Started consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.value());

        if (consumerRecord.offset() % 2 != 0) {
            throw new RuntimeException("This is really odd.");
        }

        log.info("Finished consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(), consumerRecord.offset(), consumerRecord.value());
    }

}
