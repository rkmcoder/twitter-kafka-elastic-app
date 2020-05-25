package elasticsearch;

import com.google.gson.JsonParser;
import constants.Constants;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static constants.Constants.*;

public class TwitterElasticConsumer {

	final static Logger log = LoggerFactory.getLogger(TwitterElasticConsumer.class);

	public static void main(String[] args) throws IOException {

		RestHighLevelClient restHighLevelClient = createElasticsearchClient();
		KafkaConsumer<String, String> consumer = createKafkaConsumer(TOPIC);
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for(ConsumerRecord<String, String> record : records) {
				IndexRequest indexRequest = new IndexRequest(Constants.INDEX)
						.source(record.value(), XContentType.JSON)
						.id(getIdFromTweet(record));
				log.info(indexRequest.toString());
				IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
				log.info(indexResponse.getId());
			}
		}

	}
	private static String getIdFromTweet(ConsumerRecord<String, String> record) {
		JsonParser jsonParser = new JsonParser();
		return jsonParser.parse(record.value()).getAsJsonObject().get("id_str").getAsString();
	}

	private static RestHighLevelClient createElasticsearchClient() {

		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(
						new HttpHost(ELASTICSEARCH_SERVER, PORT, SCHEME)));
		return client;
	}

	private static KafkaConsumer<String,String> createKafkaConsumer(String topic){
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET);

		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topic));
		return consumer;
	}
}
