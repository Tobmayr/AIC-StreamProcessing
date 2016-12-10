package at.ac.tuwien.aic.streamprocessing.trident;

import at.ac.tuwien.aic.streamprocessing.kafka.producer.TaxiEntryKafkaProducer;
import at.ac.tuwien.aic.streamprocessing.trident.utils.AvgSpeedHook;
import at.ac.tuwien.aic.streamprocessing.trident.utils.DistanceHook;
import at.ac.tuwien.aic.streamprocessing.trident.utils.SpeedHook;
import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.storm.TridentProcessingTopology;
import org.apache.storm.utils.Time;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Properties;

public class AbstractTridentTopologyTest {

    private static final String TOPIC = "taxi-test";

    private TridentProcessingTopology topology;
    private SpeedHook speedHook;
    private AvgSpeedHook avgSpeedHook;
    private DistanceHook distanceHook;

    private TaxiEntryKafkaProducer producer;

    @Before
    public void setup() throws Exception {
        speedHook = new SpeedHook();
        avgSpeedHook = new AvgSpeedHook();
        distanceHook = new DistanceHook();

        topology = TridentProcessingTopology.createWithTopicAndHooks(
                TOPIC,
                speedHook,
                avgSpeedHook,
                distanceHook
        );

        topology.submitLocalCluster();
        producer = new TaxiEntryKafkaProducer(TOPIC, createProducerProperties());
    }

    @After
    public void teardown() throws Exception {
        producer.close();
        topology.stop();
    }

    private Properties createProducerProperties(){
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", topology.getKafkaInstance().getKafkaConnectString());
        producerProperties.put("acks", "1");
        producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        producerProperties.put("value.serializer", "at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntrySerializer");
        return producerProperties;
    }

    public TridentProcessingTopology getTopology() {
        return topology;
    }

    public void emitTaxis(List<TaxiEntry> entries) {
        producer.produce(entries::stream);
    }

    public void wait(int timeout) {
        try {
            Time.sleep(timeout * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public SpeedHook getSpeedHook() {
        return speedHook;
    }

    public AvgSpeedHook getAvgSpeedHook() {
        return avgSpeedHook;
    }

    public DistanceHook getDistanceHook() {
        return distanceHook;
    }
}
