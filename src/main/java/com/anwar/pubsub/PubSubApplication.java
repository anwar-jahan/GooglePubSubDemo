package com.anwar.pubsub;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.anwar.pubsub.PubSubApplication.PubsubOutboundGateway;
import com.google.cloud.pubsub.v1.AckReplyConsumer;

import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.integration.outbound.PubSubMessageHandler;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;

@RestController
@SpringBootApplication
public class PubSubApplication {

	private static final Log LOGGER = LogFactory.getLog(PubSubApplication.class);

	@Autowired
	private PubsubOutboundGateway messagingGateway;
	
	public static void main(String[] args) throws IOException {
		SpringApplication.run(PubSubApplication.class, args);
	}

	@MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
	public interface PubsubOutboundGateway {

		void sendToPubsub(String text);
	}

	@Bean
	@ServiceActivator(inputChannel = "pubsubOutputChannel")
	public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
		return new PubSubMessageHandler(pubsubTemplate, "projects/doorapi-200221/topics/sample");
	}
	
	@PostMapping("/postMessage")
	public RedirectView postMessage(@RequestParam("message") String message) {
		this.messagingGateway.sendToPubsub(message);
		return new RedirectView("/");
	}

	// received message
	@Bean
	public PubSubInboundChannelAdapter messageChannelAdapter(
			@Qualifier("pubsubInputChannel") MessageChannel inputChannel, PubSubTemplate pubSubTemplate) {
		PubSubInboundChannelAdapter adapter = new PubSubInboundChannelAdapter(pubSubTemplate,
				"projects/doorapi-200221/subscriptions/rana");

		adapter.setOutputChannel(inputChannel);
		return adapter;
	}

	@Bean
	public MessageChannel pubsubInputChannel() {
		return new DirectChannel();
	}

	@Bean
	@ServiceActivator(inputChannel = "pubsubInputChannel")
	public void messageReceiver(String payload) {
		LOGGER.info("Message arrived! Payload: " + payload);
	}

	/*@Bean
	@ServiceActivator(inputChannel = "pubsubInputChannel")
	public void messageReceiver() {
		LOGGER.info("Message arrived! Payload: " + message.getPayload());
		AckReplyConsumer consumer = message.getHeaders().get(GcpPubSubHeaders.ACKNOWLEDGEMENT, AckReplyConsumer.class);
		consumer.ack();
	}
*/
}
