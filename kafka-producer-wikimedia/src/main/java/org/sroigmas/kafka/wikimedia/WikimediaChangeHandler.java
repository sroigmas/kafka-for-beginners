package org.sroigmas.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(
      WikimediaChangeHandler.class.getSimpleName());
  private final Producer<String, String> producer;
  private final String topic;

  public WikimediaChangeHandler(Producer<String, String> producer, String topic) {
    this.producer = producer;
    this.topic = topic;
  }

  @Override
  public void onOpen() {
    // nothing here
  }

  @Override
  public void onClosed() {
    producer.close();
  }

  @Override
  public void onMessage(String event, MessageEvent messageEvent) {
    LOG.info("Sending data: {}", messageEvent.getData());
    producer.send(new ProducerRecord<>(topic, messageEvent.getData()));
  }

  @Override
  public void onComment(String s) {
    // nothing here
  }

  @Override
  public void onError(Throwable throwable) {
    LOG.error("Error reading from stream: ", throwable);
  }
}
