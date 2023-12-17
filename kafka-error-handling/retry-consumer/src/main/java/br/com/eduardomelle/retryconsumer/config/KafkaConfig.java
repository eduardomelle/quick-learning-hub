package br.com.eduardomelle.retryconsumer.config;

import br.com.eduardomelle.retryconsumer.entity.FailedMessage;
import br.com.eduardomelle.retryconsumer.repository.FailedMessageRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Slf4j
@Configuration
public class KafkaConfig {

    private final KafkaProps kafkaProps;

    private final ObjectMapper objectMapper;

    private final FailedMessageRepository failedMessageRepository;

    public KafkaConfig(KafkaProps kafkaProps, ObjectMapper objectMapper, FailedMessageRepository failedMessageRepository) {
        this.kafkaProps = kafkaProps;
        this.objectMapper = objectMapper;
        this.failedMessageRepository = failedMessageRepository;
    }

    @Bean("kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory() {
        ConsumerFactory consumerFactory = new DefaultKafkaConsumerFactory(this.kafkaProps.getConsumerProps());

        ConcurrentKafkaListenerContainerFactory concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory();
        concurrentKafkaListenerContainerFactory.setConsumerFactory(consumerFactory);
        concurrentKafkaListenerContainerFactory.setCommonErrorHandler(getDefaultErrorHandler());
        return concurrentKafkaListenerContainerFactory;
    }

    private DefaultErrorHandler getDefaultErrorHandler() {
        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler((record, exception) -> {
            FailedMessage failedMessageEntity = new FailedMessage();
            try {
                failedMessageEntity.setMessage(this.objectMapper.writeValueAsString(record.value()));
                failedMessageEntity.setException(exception.getClass().toString());
                failedMessageEntity.setTopic(record.topic());
                failedMessageEntity.setConsumerOffset(record.offset());

                this.failedMessageRepository.save(failedMessageEntity);

                log.error("Saved the failed message to db {}", failedMessageEntity);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

        }, new FixedBackOff(10000L, 2L));

        return defaultErrorHandler;
    }

}
