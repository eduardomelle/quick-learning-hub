package br.com.eduardomelle.mainconsumer.controller;

import br.com.eduardomelle.mainconsumer.dto.MyDTO;
import br.com.eduardomelle.mainconsumer.publisher.KafkaPublisher;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class PublisherController {

    private final KafkaPublisher kafkaPublisher;

    public PublisherController(KafkaPublisher kafkaPublisher) {
        this.kafkaPublisher = kafkaPublisher;
    }

    @PostMapping(value = "/publish")
    public void publish(@RequestBody @Valid MyDTO dto) {
        log.info("Publishing the event {}", dto);
        this.kafkaPublisher.publish(dto);
    }

}
