package com.daniel.jsonconsumer.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.daniel.paymentService.model.Payment;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class JsonListener {
	
	@SneakyThrows
	@KafkaListener(topics = "payment-topic", groupId = "create-group", containerFactory = "jsonContainerFactory")
	public void antiFraud(@Payload Payment payment) {
		log.info("Recebi o pagamento: {}", payment);
		log.info(2000);
		log.info("Validando Fraude");
		Thread.sleep(2000);
		log.info("Compra Aprovada");
		Thread.sleep(2000);
		
	}
	
	@SneakyThrows
	@KafkaListener(topics = "payment-topic", groupId = "pdf-group", containerFactory = "jsonContainerFactory")
	public void pdfGenerateor(@Payload Payment  payment) {
		Thread.sleep(3000);
		log.info("Gerando PDF do produto de Id: {}", payment.getId());
		Thread.sleep(3000);
		
	}
	
	@SneakyThrows
	@KafkaListener(topics = "payment-topic", groupId = "email-group", containerFactory = "jsonContainerFactory")
	public void sendEmail() {
		Thread.sleep(3000);
		log.info("Enviando E-mail confirmação");		
	}


}
