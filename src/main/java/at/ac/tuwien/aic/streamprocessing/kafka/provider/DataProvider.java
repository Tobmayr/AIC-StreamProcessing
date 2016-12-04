package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntrySerializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * Data Provider
 */
public class DataProvider {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int KAFKA_PORT = 9092;
    private static final String KAFKA_URI = "localhost:" + KAFKA_PORT;


    public static void main(String[] args) throws Exception{
        if(args.length < 2){
            System.out.println("USAGE: <absolute-path-of-input-data> <topic-name>");
            return;
        }

        String filePath = args[0].toString();
        String topicName = args[1].toString();

        Properties producerProperties = createProducerProperties();

        LocalDateTime referenceDateTime = LocalDateTime.parse("2002-02-02 13:30:45", formatter);
        LocalDateTime rowDateTime = null;

        Producer<Integer, byte[]> producer = new KafkaProducer<>(producerProperties);

        try
        {
            Reader in = new FileReader(filePath);
            Iterable<CSVRecord> csvLines = CSVFormat.EXCEL.parse(in);

            for (CSVRecord csvLine : csvLines) {
                rowDateTime = LocalDateTime.parse(csvLine.get(1), formatter);

                if(referenceDateTime.equals(rowDateTime)){
                    TaxiEntry entry = new TaxiEntry(Integer.parseInt(csvLine.get(0)), LocalDateTime.parse(csvLine.get(1), formatter), Double.parseDouble(csvLine.get(2)), Double.parseDouble(csvLine.get(3)));
                    byte[] serialized = TaxiEntrySerializer.serialize(entry);
                    ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topicName, entry.getTaxiId(), serialized);
                    System.out.println("Sending :" + csvLine.get(0) + "," +  csvLine.get(1) + "," +  csvLine.get(2) + "," +  csvLine.get(3));
                    producer.send(record);
                } else {
                    Duration timeDiff = Duration.between(referenceDateTime, rowDateTime);
                    System.out.println("Sleeeping.... ");
                    Thread.sleep(1000);

                    TaxiEntry entry = new TaxiEntry(Integer.parseInt(csvLine.get(0)), LocalDateTime.parse(csvLine.get(1), formatter), Double.parseDouble(csvLine.get(2)), Double.parseDouble(csvLine.get(3)));
                    byte[] serialized = TaxiEntrySerializer.serialize(entry);
                    ProducerRecord<Integer, byte[]> record = new ProducerRecord<>(topicName, entry.getTaxiId(), serialized);
                    System.out.println("Sending:" + csvLine.get(0) + "," +  csvLine.get(1) + "," +  csvLine.get(2) + "," +  csvLine.get(3));
                    System.out.println("Minute diff:" + timeDiff.getSeconds()/60);
                    producer.send(record);

                    referenceDateTime = rowDateTime;
                }
            }
        } catch (FileNotFoundException e1) {
            e1.printStackTrace();
        } catch (IOException e1) {
            e1.printStackTrace();
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
