package org.sroigmas.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.EventSource.Builder;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class WikimediaChangesProducer {

  public static void main(String[] args) throws InterruptedException {
    String bootstrapServers = "localhost:9092";

    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    String topic = "wikimedia_recentchange";

    EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";

    EventSource eventSource = new Builder(eventHandler, URI.create(url)).build();
    eventSource.start();

    TimeUnit.MINUTES.sleep(10);
  }
}
