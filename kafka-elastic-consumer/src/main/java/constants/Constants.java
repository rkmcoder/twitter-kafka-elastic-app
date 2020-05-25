package constants;

public class Constants {

	//ElasticSearch config
	public static final String ELASTICSEARCH_SERVER = "localhost";
	public static final Integer PORT = 9200;
	public static final String SCHEME = "http";
	public static final String INDEX = "twitter";

	//Kafka Consumer config
	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static final String TOPIC = "twitter_tweets";
	public static final String CONSUMER_GROUP = "twitter-consumer-group";
	public static final String AUTO_OFFSET_RESET = "earliest";
	public static final String ENABLE_AUTO_COMMIT = "false";
	public static final String MAX_POLL_RECORDS = "10";



}
