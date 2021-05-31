package com.demo.credit;

import java.io.IOException;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Producer;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

@Service
public class CreditConsumer {

	private final Logger logger = LoggerFactory.getLogger(CreditConsumer.class);
	private static final String CONSUMER_TOPIC = "ApplicationTopic";
	private static final String PRODUCER_TOPIC = "CreditTopic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@KafkaListener(topics = CONSUMER_TOPIC, groupId = "creditConsumer")
	public void consume(ConsumerRecord<String, String> record) throws IOException, ParseException {
		JSONParser parser = new JSONParser();
		ObjectMapper mapper = new ObjectMapper();
		JSONObject json = (JSONObject) parser.parse(record.value());

		String applicationId = record.key();
		String ssn = (String) json.get("ssn");
		logger.info(String.format("Reading application %s from ApplicationTopic : \n%s", applicationId, json));

		CreditDetails creditDetails = fetchCreditDetails(applicationId, ssn);

		ProducerRecord<String, String> creditRecord = new ProducerRecord<String, String>(PRODUCER_TOPIC, applicationId,
				mapper.writeValueAsString(creditDetails));
		logger.info("User details from Credit agency : " + creditDetails);
		kafkaTemplate.send(creditRecord);

	}

	private CreditDetails fetchCreditDetails(String applicationId, String ssn) {
		// TODO Auto-generated method stub
		Faker faker = new Faker();
		Random r = new Random();
		String streetName = faker.address().streetName();
		String number = faker.address().buildingNumber();
		String city = faker.address().city();
		String country = faker.address().country();

		int low = 10000;
		int high = 70000;
		return new CreditDetails(applicationId, String.format("%s %s %s %s", number, streetName, city, country),
				new Long(r.nextInt(high - low) + low), r.nextBoolean());
	}
}