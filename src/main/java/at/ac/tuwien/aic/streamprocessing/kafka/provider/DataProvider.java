package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.kafka.producer.TaxiEntryKafkaProducer;
import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

/**
 * Data Provider provides data to Kafka (i.e. TaxiEntry objects) by parsing a CSV file containing sorted entries by timestamp
 */
public class DataProvider {
    private static final Logger logger = LoggerFactory.getLogger(DataProvider.class);

    private static final LocalDateTime REFERENCE_START_TIME = Timestamp.parse("2008-02-02 13:30:45");

    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_URI = "localhost:" + KAFKA_PORT;

    private String topic;
    private int speedFactor;

    public DataProvider(String topic, int speedFactor) {
        this.speedFactor = speedFactor;
        this.topic = topic;
    }

    private void provide(Reader reader) {
        Properties producerProperties = createProducerProperties();
        TaxiEntryKafkaProducer producer = new TaxiEntryKafkaProducer(topic, producerProperties);

        try {
            CSVParser csv = CSVFormat.EXCEL.parse(reader);

            LocalDateTime currentBatchStart = REFERENCE_START_TIME;
            LocalDateTime nextBatchStart;
            TaxiEntry last = null;

            Iterator<CSVRecord> recordIterator = csv.iterator();

            while (recordIterator.hasNext()) {
                Batch batch = getNextBatch(recordIterator, last, currentBatchStart);
                producer.produce(batch.entries::stream);

                if (batch.last != null) {
                    nextBatchStart = batch.last.getTimestamp();

                    // compute wait time between now and next entry
                    long seconds = ChronoUnit.SECONDS.between(currentBatchStart, nextBatchStart);

                    currentBatchStart = nextBatchStart;
                    last = batch.last;

                    // simulate waiting for next entry
                    try {
                        Time.sleep((seconds * 1000) / speedFactor);
                    } catch (InterruptedException e) {

                    }
                }
            }
        } catch (IOException e) {
            logger.error("Filed reading the file!", e);
        } finally {
            producer.close();
        }
    }

    private Batch getNextBatch(Iterator<CSVRecord> recordIterator, TaxiEntry startEntry, LocalDateTime until) {
        List<TaxiEntry> entries = new ArrayList<>();

        if (startEntry != null) {
            entries.add(startEntry);
        }

        while (recordIterator.hasNext()) {
            CSVRecord record = recordIterator.next();
            TaxiEntry entry = parseCsvRecord(record);

            if (entry == null) {
                // malformed record, ignore
                continue;
            }

            if (entry.getTimestamp().isAfter(until)) {
                // found end of current batch
                return new Batch(entries, entry);
            }

            long sameEntryCount = entries.stream()
                    .filter(e -> e.getTaxiId() == entry.getTaxiId())
                    .filter(e -> e.getTimestamp().isEqual(entry.getTimestamp()))
                    .count();

            if (sameEntryCount == 0) {
                entries.add(entry);
            } else {
                // duplicate entries for one taxi at the same moment
                // disregard it as it will computing meaningful values impossible
                logger.debug("Filtered same-time entry for taxi " + entry.getTaxiId() + ": " + entry.toString());
            }
        }

        return new Batch(entries, null);
    }

    private TaxiEntry parseCsvRecord(CSVRecord record) {
        try {
            Integer taxiId = Integer.parseInt(record.get(0));
            LocalDateTime timestamp = Timestamp.parse(record.get(1));
            double latitude = Double.parseDouble(record.get(2));
            double longitude = Double.parseDouble(record.get(3));

            return new TaxiEntry(taxiId, timestamp, latitude, longitude);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse record '" + record.toString() + "'");
            return null;
        }
    }

    private Properties createProducerProperties(){
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", KAFKA_URI);
        producerProperties.put("acks", "1");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProperties.put("value.serializer", "at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntrySerializer");
        return producerProperties;
    }

    private static class Batch {
        List<TaxiEntry> entries;
        TaxiEntry last;

        Batch(List<TaxiEntry> entries, TaxiEntry last) {
            this.entries = entries;
            this.last = last;
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("USAGE: <absolute-path-of-input-data> <topic-name> <speed-factor-to-divide-seconds>");
            return;
        }

        String filePath = args[0];
        String topic = args[1];
        int speedFactor = Integer.parseInt(args[2]);

        if (speedFactor <= 0) {
            logger.error("Speed factor should be a positive number");
            return;
        }

        try {
            Reader reader = new FileReader(filePath);

            DataProvider provider = new DataProvider(topic, speedFactor);
            provider.provide(reader);
        } catch (FileNotFoundException e1) {
            logger.error("File with the given path could not be found!", e1);
        }
    }
}
