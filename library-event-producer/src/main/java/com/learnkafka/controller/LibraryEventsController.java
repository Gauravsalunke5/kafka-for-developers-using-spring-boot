package com.learnkafka.controller;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.domain.LibraryEventType;
import com.learnkafka.producer.LibraryEventProducer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
@RequestMapping("/v1")
public class LibraryEventsController {

	@Autowired
	LibraryEventProducer libraryEventProducer;

	@PostMapping("/libraryevent/app1")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException {
		// invoke kafka producer
		libraryEventProducer.sendLibraryEvent(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);

	}

	@PostMapping("/libraryevent/sync")
	public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
		// invoke kafka producer
		SendResult<Integer, String> result = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
		log.info("Message sent successfully and response is {}", result.toString());
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);

	}

	@PostMapping("/libraryevent")
	public ResponseEntity<LibraryEvent> postLibraryEvent_approach2(@RequestBody LibraryEvent libraryEvent)
			throws JsonProcessingException {
		// invoke kafka producer
		libraryEvent.setLibraryEventType(LibraryEventType.NEW);
		libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);

	}

	@PutMapping("/libraryevent")
	public ResponseEntity<?> putLibraryEvent(@RequestBody @Validated LibraryEvent libraryEvent)
			throws JsonProcessingException, ExecutionException, InterruptedException {

		if (libraryEvent.getLibraryEventId() == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please pass the LibraryEventId");
		}

		libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
		libraryEventProducer.sendLibraryEvent_approach2(libraryEvent);
		return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
	}
}
