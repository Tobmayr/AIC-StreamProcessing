package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntrySerializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Data Provider provides data to Kafka (i.e. TaxiEntry objects) by parsing a CSV file containing sorted entries by timestamp
 */
public class DataProvider {

    private static final Logger logger = LoggerFactory.getLogger(DataProvider.class);

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_URI = "localhost:" + KAFKA_PORT;


    public static void main(String[] args) throws Exception{
        if(args.length < 3){
            System.out.println("USAGE: <absolute-path-of-input-data> <topic-name> <time-factor-to-divide-seconds>");
            return;
        } else if(Integer.parseInt(args[2]) <= 0){
            System.out.println("Time factor should be a positive number");
        }

        String filePath = args[0];
        String topicName = args[1];
        int timeFactor = Integer.parseInt(args[2]);

        Properties producerProperties = createProducerProperties();
        Producer<Integer, byte[]> producer = new KafkaProducer<>(producerProperties);

        try
        {
            Reader in = new FileReader(filePath);
            Iterable<CSVRecord> csvLines = CSVFormat.EXCEL.parse(in);

            LocalDateTime referenceDateTime = LocalDateTime.parse("2008-02-02 13:30:45", formatter);
            LocalDateTime rowDateTime = null;

            int counter = 0;
            for (CSVRecord csvLine : csvLines) {
                rowDateTime = LocalDateTime.parse(csvLine.get(1), formatter);
                if(referenceDateTime.equals(rowDateTime)){
                    TaxiEntry entry = new TaxiEntry(Integer.parseInt(csvLine.get(0)), LocalDateTime.parse(csvLine.get(1), formatter), Double.parseDouble(csvLine.get(2)), Double.parseDouble(csvLine.get(3)));
                    byte[] serialized = TaxiEntrySerializer.serialize(entry);
                    ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topicName, entry.getTaxiId(), serialized);
                    logger.info("Sending :" + csvLine.get(0) + "," +  csvLine.get(1) + "," +  csvLine.get(2) + "," +  csvLine.get(3));
                    producer.send(record);
                    counter++;
                } else {
                    Duration timeDiff = Duration.between(referenceDateTime, rowDateTime);
                    logger.info("Sleeping.... ");
                    Thread.sleep(Math.round(timeDiff.getSeconds()/timeFactor));

                    TaxiEntry entry = new TaxiEntry(Integer.parseInt(csvLine.get(0)), LocalDateTime.parse(csvLine.get(1), formatter), Double.parseDouble(csvLine.get(2)), Double.parseDouble(csvLine.get(3)));
                    byte[] serialized = TaxiEntrySerializer.serialize(entry);
                    ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topicName, entry.getTaxiId(), serialized);
                    logger.info("Sending:" + csvLine.get(0) + "," +  csvLine.get(1) + "," +  csvLine.get(2) + "," +  csvLine.get(3));
                    producer.send(record);
                    counter++;

                    referenceDateTime = rowDateTime;
                }
            }
            logger.info("Submitted " + counter + " entries");
        } catch (FileNotFoundException e1) {
            logger.error("File with the given path could not be found! " +  e1.toString());
        } catch (IOException e1) {
            logger.error("Filed reading the file! " +  e1.toString());
        } finally {
            producer.close();
        }
    }

    private static Properties createProducerProperties(){
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", KAFKA_URI);
        producerProperties.put("acks", "1");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return producerProperties;
    }
}
