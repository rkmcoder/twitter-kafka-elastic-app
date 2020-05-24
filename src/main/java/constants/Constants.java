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

	//Kafka config
	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String TOPIC = "twitter_tweets";

}
