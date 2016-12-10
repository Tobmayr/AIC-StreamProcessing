package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TaxiEntryKeyValueScheme;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentProcessingTopology {
    private static final String SPOUT_ID = "kafka-spout";

    private String topic;

    private final String redisHost;
    private final int redisPort;

    private final BaseFilter speedHook;
    private final BaseFilter avgSpeedHook;
    private final BaseFilter distanceHook;

    private LocalKafkaInstance localKafkaInstance;
    private TridentTopology topology;

    public TridentProcessingTopology(String topic, String redisHost, int redisPort) {
        this.topic = topic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;

        this.speedHook = null;
        this.avgSpeedHook = null;
        this.distanceHook = null;
    }

    public TridentProcessingTopology(String topic, String redisHost, int redisPort, BaseFilter speedHook, BaseFilter avgSpeedHook, BaseFilter distanceHook) {
        this.topic = topic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.speedHook = speedHook;
        this.avgSpeedHook = avgSpeedHook;
        this.distanceHook = distanceHook;
    }

    public void startKafka() {
        localKafkaInstance = new LocalKafkaInstance(9092, 2000);

        try {
            localKafkaInstance.start();
        } catch (Exception e) {
            System.out.println("Caught exception while starting kafka. Aborting");
            e.printStackTrace();

            System.exit(1);
        }

        localKafkaInstance.createTopic(topic);
    }

    public void stopKafka() {
        try {
            localKafkaInstance.stop();
        } catch (Exception e) {
            System.out.println("Caught exception while stopping kafka. Aborting");
            e.printStackTrace();

            System.exit(1);
        }
    }

    public StormTopology build() {
        if (localKafkaInstance == null) {
            throw new IllegalStateException("Must start kafka before building topology");
        }

        topology = new TridentTopology();

        OpaqueTridentKafkaSpout spout = buildKafkaSpout();


        // setup topology
        Stream inputStream = topology.newStream(SPOUT_ID, spout);

        // setup speed aggregator
        Stream speedStream = inputStream
                .partitionAggregate(TridentFields.base, new CalculateSpeed(), TridentFields.baseAndSpeed)
                .toStream();

        if (speedHook != null) {
            speedStream = speedStream.each(TridentFields.baseAndSpeed, speedHook);
        }

        // setup average speed aggregator
        speedStream = speedStream
                .partitionAggregate(TridentFields.baseAndSpeed, new CalculateAverageSpeed(), TridentFields.baseAndSpeedAndAvgSpeed)
                .toStream();

        if (avgSpeedHook != null) {
            speedStream = speedStream.each(TridentFields.baseAndSpeedAndAvgSpeed, avgSpeedHook);
        }

        // TODO: enable
        // speedStream.each(taxiFieldsWithAvgSpeed, new StoreInformation(OperatorType.AVERGAGE_SPEED));

        // setup distance aggregator
        Stream distanceStream = inputStream
                .partitionAggregate(TridentFields.base, new CalculateDistance(), TridentFields.distance);

        if (distanceHook != null) {
            distanceStream = distanceStream.toStream()
                    .each(TridentFields.distance, distanceHook);
        }

        // TODO: enable
        // distanceStream.each(taxiFieldsWithDistance, new StoreInformation(OperatorType.DISTANCE));

        return topology.build();
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout() {
        ZkHosts zkHosts = new ZkHosts(localKafkaInstance.getConnectString());
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(zkHosts, topic);
        spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TaxiEntryKeyValueScheme());

        return new OpaqueTridentKafkaSpout(spoutConfig);
    }

    public void runLocalCluster(int timeout) {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        startKafka();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, build());

        try {
            Thread.sleep(timeout * 1000);
        } catch (InterruptedException e) {

        }

        cluster.shutdown();
        stopKafka();
    }

    public static TridentProcessingTopology createWithHooks(BaseFilter speedHook, BaseFilter avgSpeedHook, BaseFilter distanceHook) throws Exception {
        return new TridentProcessingTopology(
                "taxi", "localhost", 6379,
                speedHook, avgSpeedHook, distanceHook);
    }

    public static void main(String[] args) throws Exception {
        BaseFilter speedHook = new Debug("speed");
        BaseFilter avgSpeedHook = new Debug("avgSpeed");
        BaseFilter distanceHook = new Debug("distance");

        TridentProcessingTopology topology = createWithHooks(speedHook, avgSpeedHook, distanceHook);
        topology.runLocalCluster(60);
    }

    public static class TridentFields {
        static Fields base = new Fields("id", "timestamp", "latitude", "longitude");
        static Fields baseAndSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed");
        static Fields baseAndSpeedAndAvgSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
        static Fields distance = new Fields("id", "timestamp", "latitude", "longitude", "distance");
    }

}
