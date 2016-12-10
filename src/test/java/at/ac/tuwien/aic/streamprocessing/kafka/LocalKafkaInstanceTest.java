package at.ac.tuwien.aic.streamprocessing.kafka;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.KafkaTestConfiguration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class LocalKafkaInstanceTest extends AbstractLocalKafkaInstanceTest {
    private static final String TOPIC = "simple";

    public LocalKafkaInstanceTest() {
        super(Collections.singletonList(TOPIC));
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void consume_shouldReceive_producedMessages() {
        // produce some messages
        Producer<String, String> producer = new KafkaProducer<>(KafkaTestConfiguration.createProducerProperties());
        for(int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), Integer.toString(i)));
        }
        producer.close();

        // consume them
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(KafkaTestConfiguration.createConsumerProperties());
            consumer.subscribe(Collections.singletonList(TOPIC));

            Integer i = 0;
            while (i < 100) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    assertEquals(i.toString(), record.key());
                    i = i + 1;
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
