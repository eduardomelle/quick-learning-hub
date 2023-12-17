package br.com.eduardomelle.retryconsumer.listener;

import br.com.eduardomelle.retryconsumer.dto.MyDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@NoArgsConstructor
@Slf4j
@Service
public class RetryMessageListener {

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> consumerRecord) {
        log.info("Started consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(),
                consumerRecord.offset(), consumerRecord.value());

        if (consumerRecord.offset() % 2 != 0) {
            throw new IllegalStateException("This is something odd.");
        }

        try {
            MyDTO myDTO = this.objectMapper.readValue(consumerRecord.value(), MyDTO.class);
            log.info("Finished consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(),
                    consumerRecord.offset(), myDTO);
            // do something with the deserialized object
        } catch (Exception e) {
            log.error(e.getMessage());
        }

    }

}
