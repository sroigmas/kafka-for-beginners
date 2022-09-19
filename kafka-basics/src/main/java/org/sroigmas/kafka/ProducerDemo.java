package org.sroigmas.kafka;

import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

  private static final Logger LOG = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    String topic = "demo_java";
    String key = "id_0";
    String msg = "hello world";
    //ProducerRecord<String, String> record = new ProducerRecord<>(topic, msg);
    ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, msg);

    //producer.send(record);
    producer.send(record, (recordMetadata, e) -> {
      if (Objects.isNull(e)) {
        LOG.info("Received new metadata\n"
            + "\tTopic: " + recordMetadata.topic() + "\n"
            + "\tKey: " + record.key() + "\n"
            + "\tPartition: " + recordMetadata.partition() + "\n"
            + "\tOffset: " + recordMetadata.offset() + "\n"
            + "\tTimestamp: " + recordMetadata.timestamp());
      } else {
        LOG.error("Error sending record: ", e);
      }
    });
    //producer.flush();
    producer.close();
  }
}
