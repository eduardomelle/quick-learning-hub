/**
 * 
 */
package br.com.eduardomelle.mainconsumer.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
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

import lombok.extern.slf4j.Slf4j;

/**
 * 
 */
@Slf4j
@Configuration
public class KafkaConfig {

	@Value(value = "${spring.kafka.dead_letter_topic:}")
	private String deadLetterTopic;

	@Autowired
	private KafkaProps kafkaProps;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory() {
		DefaultKafkaConsumerFactory defaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory(
				this.kafkaProps.consumerProps());

		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = new DeadLetterPublishingRecoverer(
				this.kafkaTemplate, (record, ex) -> {
					log.info("Exception {} occurred sending the record to the error topic {}", ex.getMessage(),
							deadLetterTopic);
					return new TopicPartition(this.deadLetterTopic, -1);
				});

		CommonErrorHandler commonErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer,
				new FixedBackOff(0L, 1L));

		ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
		concurrentKafkaListenerContainerFactory.setConsumerFactory(defaultKafkaConsumerFactory);
		concurrentKafkaListenerContainerFactory.setCommonErrorHandler(commonErrorHandler);
		return concurrentKafkaListenerContainerFactory;
	}

}
