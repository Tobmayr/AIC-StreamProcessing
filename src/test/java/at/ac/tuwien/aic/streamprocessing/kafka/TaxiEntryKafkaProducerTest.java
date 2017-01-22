package at.ac.tuwien.aic.streamprocessing.kafka;

import at.ac.tuwien.aic.streamprocessing.kafka.producer.TaxiEntryKafkaProducer;
import at.ac.tuwien.aic.streamprocessing.kafka.utils.KafkaTestConfiguration;
import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntryDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class TaxiEntryKafkaProducerTest extends AbstractLocalKafkaInstanceTest {

    private static final String TOPIC = "taxi-entries";

    public TaxiEntryKafkaProducerTest() {
        super(Collections.singletonList(TOPIC));
    }

    @Test(timeout = DEFAULT_TEST_TIMEOUT)
    public void consume_shouldReceived_producedEntries() {
        List<TaxiEntry> expected = Arrays.asList(
                new TaxiEntry(1, LocalDateTime.now(), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(5), 10.5, 9.5),
                new TaxiEntry(2, LocalDateTime.now().plusMinutes(10), 100.0, 100.0)
        );

        Properties producerProperties = KafkaTestConfiguration.createProducerProperties();
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProperties.put("value.serializer", "at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntrySerializer");
        TaxiEntryKafkaProducer producer = new TaxiEntryKafkaProducer(TOPIC, producerProperties);

        producer.produce(expected::stream);
        producer.close();

        // consume them
        KafkaConsumer<Integer, byte[]> consumer = null;
        try {
            Properties consumerProperties = KafkaTestConfiguration.createConsumerProperties();
            consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
            consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumer = new KafkaConsumer<>(consumerProperties);
            consumer.subscribe(Collections.singletonList(TOPIC));

            List<ConsumerRecord<Integer, byte[]>> consumed = consume(consumer, expected.size());
            List<TaxiEntry> actual = consumed.stream().map(r -> TaxiEntryDeserializer.deserialize(r.value()))
                    .sorted(Comparator.comparing(TaxiEntry::getTimestamp))
                    .collect(Collectors.toList());

            assertThat(actual, is(expected));
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    private List<ConsumerRecord<Integer, byte[]>> consume(KafkaConsumer<Integer, byte[]> consumer, int minRecords) {
        List<ConsumerRecord<Integer, byte[]>> consumed = new ArrayList<>();
        while (consumed.size() < minRecords) {
            ConsumerRecords<Integer, byte[]> records = consumer.poll(100);

            for (ConsumerRecord<Integer, byte[]> record : records) {
                consumed.add(record);
            }
        }

        return consumed;
    }
}
