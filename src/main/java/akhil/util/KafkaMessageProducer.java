package akhil.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;

import akhil.DataUnlimited.util.LogStackTrace;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;

public class KafkaMessageProducer {
	final static String CLIENT_ID = "dmsvnspservicevirtualization";
	static long suffix = 0;

	private Producer<String, String> producer;

	public void close() {
		producer.close();
	}

	public RecordMetadata send(ProducerRecord<String, String> record) {

		try {
			return producer.send(record).get();
		} catch (InterruptedException e) {
			String msg = this.brokerUrl + ":" + this.oauthClientId + ":" + this.oauthClientSecret + ":"
					+ this.oauthTokenUrl + ":" + this.compression + "\n";
			LOGGER.log(Level.SEVERE,
					"ERROR: InterruptedException in posting message:" + msg + LogStackTrace.get(e) + "\n");
			return null;
		} catch (ExecutionException e) {
			String msg = this.brokerUrl + ":" + this.oauthClientId + ":" + this.oauthClientSecret + ":"
					+ this.oauthTokenUrl + ":" + this.compression + "\n";
			LOGGER.log(Level.SEVERE,
					"ERROR: ExecutionException in posting message:" + msg + LogStackTrace.get(e) + "\n");
			return null;
		}
	}

	private String brokerUrl;
	private String oauthTokenUrl;
	private String oauthClientId;
	private String oauthClientSecret;
	private String compression;

	public KafkaMessageProducer(String brokerUrl, String oauthTokenUrl, String oauthClientId, String oauthClientSecret,
			String compression) {
		this.brokerUrl = brokerUrl;
		this.oauthTokenUrl = oauthTokenUrl;
		this.oauthClientId = oauthClientId;
		this.oauthClientSecret = oauthClientSecret;
		this.compression = compression;
		System.setProperty("oauth.token.endpoint.uri", oauthTokenUrl);
		System.setProperty("oauth.client.id", oauthClientId);
		System.setProperty("oauth.client.secret", oauthClientSecret);
		// Connection
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "_" + new Date().getTime());
		// Connection
		props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
		props.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");
		props.put(SaslConfigs.SASL_JAAS_CONFIG,
				"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
		props.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
				"io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");

		// Serialization / Deserialization
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		if (!compression.equals("none"))
			props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);

		// TODO: If using a VPC Endpoint to connect to NSP then these settings will need
		// to be applied
		// props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
		// props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, JKS_LOCATION);

		// Return the producer
		producer = new KafkaProducer<>(props);
	}

	private static java.util.logging.Logger LOGGER = java.util.logging.Logger
			.getLogger((KafkaMessageProducer.class).getName());

}
