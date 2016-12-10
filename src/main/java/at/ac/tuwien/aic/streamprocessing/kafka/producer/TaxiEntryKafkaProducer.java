package at.ac.tuwien.aic.streamprocessing.kafka.producer;

import at.ac.tuwien.aic.streamprocessing.kafka.provider.TaxiEntryProvider;
import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Producer which supplies records of taxi entries into a given kafka topic.
 */
public class TaxiEntryKafkaProducer {
    private final Logger logger = LoggerFactory.getLogger(TaxiEntryKafkaProducer.class);

    private String topic;
    private KafkaProducer<Integer, TaxiEntry> producer;

    /**
     * Instantiates a new TaxiEntryKafkaProducer.
     *
     * @param topic           the topic into which the records should be produced.
     * @param kafkaProperties the kafka properties describing the broker.
     */
    public TaxiEntryKafkaProducer(String topic, Properties kafkaProperties) {
        this.topic = topic;
        this.producer = new KafkaProducer<>(kafkaProperties);
    }

    /**
     * Close the kafka producer.
     */
    public void close() {
        producer.close();
    }

    /**
     * Produce one record for each taxi entry supplied by the given provider.
     *
     * @param provider the provider supplying the taxi entries.
     */
    public void produce(TaxiEntryProvider provider) {
        provider.getEntries().forEach(entry -> {
            logger.debug("Produce " + entry.toString());
            ProducerRecord<Integer, TaxiEntry> record = new ProducerRecord<>(topic, entry.getTaxiId(), entry);
            producer.send(record);
        });
    }
}
