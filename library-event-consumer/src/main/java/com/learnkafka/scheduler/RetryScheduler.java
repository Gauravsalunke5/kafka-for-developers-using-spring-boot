package com.learnkafka.scheduler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.service.LibraryEventsService;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class RetryScheduler {

	private FailureRecordRepository failureRecordRepository;
	private LibraryEventsService libraryEventsService;

	public RetryScheduler(FailureRecordRepository failureRecordRepository, LibraryEventsService libraryEventsService) {
		this.failureRecordRepository = failureRecordRepository;
		this.libraryEventsService = libraryEventsService;
	}

	@Scheduled(fixedRate = 10000)
	public void retryFailedRecords() {

		log.info("Retrying Failed Records Started!");
		var status = LibraryEventConsumerConfig.RETRY;
		failureRecordRepository.findAllByStatus(status).forEach(failureRecord -> {
			try {
				// libraryEventsService.processLibraryEvent();
				var consumerRecord = buildConsumerRecord(failureRecord);
				libraryEventsService.processLibraryEvent(consumerRecord);
				// libraryEventsConsumer.onMessage(consumerRecord); // This does not involve the
				// recovery code for in the consumerConfig
				
				failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
				failureRecordRepository.save(failureRecord);
			} catch (Exception e) {
				log.error("Exception in retryFailedRecords : ", e);
			}

		});

	}

	private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

		return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition(),
				failureRecord.getOffsetValue(), failureRecord.getKeyValue(), failureRecord.getErrorRecord());

	}
}
