package kafka;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
		Client client = createTwitterClient(msgQueue);

		log.info("Attempting to connect twitter api....");
		client.connect();
		log.info("Connection established!!!!");

		int x = 0;

		while(!client.isDone() && x < 5) {
			String tweet = null;

			try {
				tweet = msgQueue.poll(5, TimeUnit.SECONDS);
				x++;
			} catch (InterruptedException e) {
				client.stop();
				e.printStackTrace();
			}

			if (tweet != null) {
				System.out.println(tweet);
			}
		}

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		List<String> terms = Lists.newArrayList("coronavirus", "covid-19");
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
