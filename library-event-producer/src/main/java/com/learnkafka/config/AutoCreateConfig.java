package com.learnkafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
// create topic programmatically , not recommended on prod so profile local
@Profile("local")
public class AutoCreateConfig {

	@Bean
	public NewTopic libraryEvents() {
		return TopicBuilder.name("library-events").partitions(2).replicas(2).build();
	}

}
