package elasticsearch;

import constants.Constants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static constants.Constants.*;

public class TwitterElasticConsumer {

	final static Logger log = LoggerFactory.getLogger(TwitterElasticConsumer.class);

	public static void main(String[] args) throws IOException {

		RestHighLevelClient restHighLevelClient = createElasticsearchClient();
		String json = "{\"foo\" : \"bar\"}";
		IndexRequest indexRequest = new IndexRequest(Constants.INDEX).source(json, XContentType.JSON);

		log.info(indexRequest.toString());
		IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
		log.info(indexResponse.getId());

	}

	private static RestHighLevelClient createElasticsearchClient() {

		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(
						new HttpHost(ELASTICSEARCH_SERVER, PORT, SCHEME)));

		return client;
	}
}
