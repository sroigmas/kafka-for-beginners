import com.launchdarkly.eventsource.MessageEvent;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.sroigmas.kafka.wikimedia.WikimediaChangeHandler;

public class WikimediaChangeHandlerTest {

  @Test
  public void testOnMessage() {
    final StringSerializer stringSerializer = new StringSerializer();
    final MockProducer<String, String> mockProducer = new MockProducer<>(true,
        stringSerializer, stringSerializer);
    final String topic = "wikimedia_recentchange";
    final WikimediaChangeHandler handler = new WikimediaChangeHandler(mockProducer, topic);

    List<String> expectedValues = Arrays.asList("a", "b", "c");
    expectedValues.forEach(value -> handler.onMessage("event", new MessageEvent(value)));
    List<String> actualValues = mockProducer.history().stream().map(ProducerRecord::value)
        .collect(Collectors.toList());
    mockProducer.close();

    Assertions.assertEquals(expectedValues, actualValues);
  }
}
