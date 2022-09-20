package org.sroigmas.kafka.opensearch;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(
      OpenSearchConsumer.class.getSimpleName());

  public static void main(String[] args) throws IOException {
    RestHighLevelClient openSearchClient = createOpenSearchClient();
    KafkaConsumer<String, String> consumer = createKafkaConsumer();

    try (openSearchClient; consumer) {
      boolean indexExists = openSearchClient.indices()
          .exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);

      if (!indexExists) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        LOG.info("The Wikimedia index has been created!");
      } else {
        LOG.info("The Wikimedia index already exists");
      }

      consumer.subscribe(Collections.singleton("wikimedia_recentchange"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

        int recordCount = records.count();
        LOG.info("Received " + recordCount + " record(s)");

        BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : records) {
          try {
            String id = extractId(record.value());

            IndexRequest indexRequest = new IndexRequest("wikimedia")
                .source(record.value(), XContentType.JSON)
                .id(id);

            bulkRequest.add(indexRequest);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }

        if (bulkRequest.numberOfActions() > 0) {
          BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
          LOG.info("Inserted " + bulkResponse.getItems().length + " record(s)");

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

          consumer.commitSync(); // commit offsets after the batch is consumed
          LOG.info("Offsets have been committed!");
        }
      }
    }
  }

  public static RestHighLevelClient createOpenSearchClient() {
    RestHighLevelClient restHighLevelClient;
    String connString = "http://localhost:9200";
    URI connUri = URI.create(connString);
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient = new RestHighLevelClient(
          RestClient.builder(
                  new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
              .setHttpClientConfigCallback(
                  httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                      .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }

  private static KafkaConsumer<String, String> createKafkaConsumer() {
    String boostrapServers = "127.0.0.1:9092";
    String groupId = "consumer-opensearch-demo";

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    return new KafkaConsumer<>(properties);
  }

  private static String extractId(String json) {
    return JsonParser.parseString(json)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }
}
