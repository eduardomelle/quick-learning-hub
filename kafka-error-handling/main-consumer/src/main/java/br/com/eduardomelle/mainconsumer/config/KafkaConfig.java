/**
 * 
 */
package br.com.eduardomelle.mainconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 
 */
@Slf4j
@Configuration
public class KafkaConfig {

	@Value(value = "${spring.kafka.dead_letter_topic:}")
	private String deadLetterTopic;

	private final KafkaProps kafkaProps;

	private final KafkaTemplate<String, String> kafkaTemplate;

	public KafkaConfig(KafkaProps kafkaProps, KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaProps = kafkaProps;
		this.kafkaTemplate = kafkaTemplate;
	}

	@Bean("kafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory() {
		DefaultKafkaConsumerFactory defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory(
				this.kafkaProps.consumerProps());

		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(
				this.kafkaTemplate, (record, ex) -> {
					log.info("Exception {} occurred sending the record to the error topic {}", ex.getMessage(),
							this.deadLetterTopic);

					TopicPartition topicPartition = new TopicPartition(this.deadLetterTopic, -1);
					return topicPartition;
				});

		FixedBackOff fixedBackOff = new FixedBackOff(0L, 1L);

		CommonErrorHandler commonErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer, fixedBackOff);

		ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		concurrentKafkaListenerContainerFactory.setConsumerFactory(defaultKafkaConsumerFactory);
		concurrentKafkaListenerContainerFactory.setCommonErrorHandler(commonErrorHandler);
		return concurrentKafkaListenerContainerFactory;
	}

}
