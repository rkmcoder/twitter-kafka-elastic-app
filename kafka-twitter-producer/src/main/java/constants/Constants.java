package constants;

import com.google.common.collect.Lists;

import java.util.List;

public class Constants {

	//Twitter api
	public static final String API_KEY = "";
	public static final String API_SECRET_KEY = "";
	public static final String ACCESS_TOKEN = "";
	public static final String ACCESS_TOKEN_SECRET = "";

	//Search terms for tweets
	public static final List<String> terms = Lists.newArrayList("coronavirus", "covid-19");

	//Kafka Producer config
	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String TOPIC = "twitter_tweets";
	public static final String IDEMPOTENCE = "true";
	public static final String ACKS = "all";
	public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "5";
	public static final String COMPRESSION = "snappy";
	public static final String LINGER_MS = "20";
	public static final Integer BATCH_SIZE = 32; //in KB

}
