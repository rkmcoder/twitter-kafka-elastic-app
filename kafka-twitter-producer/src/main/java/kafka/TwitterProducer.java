package kafka;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.twitter.hbc.core.Constants.STREAM_HOST;
import static constants.Constants.*;

public class TwitterProducer {

	Logger log = LoggerFactory.getLogger(TwitterProducer.class);

	public TwitterProducer() {
	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

	private void run() {
		log.info("twitter-kafka-elastic-app stated....");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue(1000);
		final Client client = createTwitterClient(msgQueue);

		log.info("Attempting to connect twitter api....");
		client.connect();
		log.info("Connection established!!!!");

		final KafkaProducer<String, String> producer = createProducer();

		// shutdown hook to stop the client and close producer
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Shutting down application....");
				client.stop();
				producer.close();
				log.info("Done!!!!");
		}));

		while(!client.isDone()) {
			String tweet = null;
			try {
				tweet = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				client.stop();
				log.error("Error Occurred", e);
			}

			if (tweet != null) {
				log.info(tweet);
				producer.send(new ProducerRecord<>(TOPIC, null, tweet), new Callback() {
					public void onCompletion(RecordMetadata recordMetadata, Exception e) {
						if(e != null){
							log.error("Error Occurred", e);
						}
					}
				});
			}
		}

	}

	private KafkaProducer<String, String> createProducer() {
		Properties properties = new Properties();
		// create basic properties
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, IDEMPOTENCE);
		properties.setProperty(ProducerConfig.ACKS_CONFIG, ACKS);//idempotence default
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));//idempotence default
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);//idempotence default - controls how many producer requests can be made a parallel to a single broker.

		// high throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION);
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS);
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(BATCH_SIZE * 1024));
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		return producer;
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		hosebirdEndpoint.trackTerms(terms);
		Authentication hosebirdAuth = new OAuth1(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();
		return hosebirdClient;
	}
}
