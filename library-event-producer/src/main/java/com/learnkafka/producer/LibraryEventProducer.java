package com.learnkafka.producer;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	private static final String TOPIC_LIBRARY_EVENTS = "library-events";

	@Autowired
	KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	ObjectMapper objectMapper;

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

//		send by default topic added in application.yml file
//		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);

//		send by topic name "library-events"
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(TOPIC_LIBRARY_EVENTS, key,
				value);

		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}
		});
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
			throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.send(TOPIC_LIBRARY_EVENTS, key, value).get(1, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("ExecutionException/InterruptedException Sending the Message and the exception is {}",
					e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception Sending the Message and the exception is {}", e.getMessage());
			throw e;
		}

		return sendResult;

	}

	public void sendLibraryEvent_approach2(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, TOPIC_LIBRARY_EVENTS);

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);

		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);

			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}
		});
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topicName) {
		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topicName, null, key, value, recordHeaders);

	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {

		log.info("Message sent successfully for the key: {} and value  is {} , partition is {}", key, value,
				result.getRecordMetadata().partition());

	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending message and exception is {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable throwable) {
			log.error("error on failure: {}", throwable.getMessage());
		}

	}

}
