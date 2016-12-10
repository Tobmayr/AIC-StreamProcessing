package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TaxiEntryKeyValueScheme;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.tuple.TaxiFields;
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

public class TridentProcessingTopology {
    private static final String SPOUT_ID = "kafka-spout";

    private String topic;

    private final String redisHost;
    private final int redisPort;

    private final BaseFilter speedTupleListener;
    private final BaseFilter avgSpeedTupleListener;
    private final BaseFilter distanceTupleListener;

    private LocalKafkaInstance localKafkaInstance;
    private TridentTopology topology;
    private LocalCluster cluster;

    private boolean stopped = false;

    public TridentProcessingTopology(String topic, String redisHost, int redisPort) {
        this.topic = topic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;

        this.speedTupleListener = null;
        this.avgSpeedTupleListener = null;
        this.distanceTupleListener = null;
    }

    public TridentProcessingTopology(String topic, String redisHost, int redisPort, BaseFilter speedTupleListener, BaseFilter avgSpeedTupleListener, BaseFilter distanceTupleListener) {
        this.topic = topic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.speedTupleListener = speedTupleListener;
        this.avgSpeedTupleListener = avgSpeedTupleListener;
        this.distanceTupleListener = distanceTupleListener;
    }

    public void stop() {
        if (stopped) {
            return;
        }

        stopped = true;

        try {
            cluster.shutdown();
            stopKafka();
        } catch (Exception e) {
            System.out.println("Failed to stop cluster.");
            e.printStackTrace();

            System.exit(1);
        }
    }

    private void startKafka() {
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

    private void stopKafka() {
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
                .partitionAggregate(TaxiFields.BASE_FIELDS, new CalculateSpeed(), TaxiFields.BASE_SPEED_FIELDS)
                .toStream();

        if (speedTupleListener != null) {
            speedStream = speedStream.each(TaxiFields.BASE_SPEED_FIELDS, speedTupleListener);
        }

        // setup average speed aggregator
        speedStream = speedStream
                .partitionAggregate(TaxiFields.BASE_SPEED_FIELDS, new CalculateAverageSpeed(), TaxiFields.BASE_SPEED_AVG_FIELDS)
                .toStream();

        if (avgSpeedTupleListener != null) {
            speedStream = speedStream.each(TaxiFields.BASE_SPEED_AVG_FIELDS, avgSpeedTupleListener);
        }

        // TODO: enable
        // speedStream.each(taxiFieldsWithAvgSpeed, new StoreInformation(OperatorType.AVERGAGE_SPEED));

        // setup distance aggregator
        Stream distanceStream = inputStream
                .partitionAggregate(TaxiFields.BASE_FIELDS, new CalculateDistance(), TaxiFields.BASE_DISTANCE_FIELDS);

        if (distanceTupleListener != null) {
            distanceStream = distanceStream.toStream()
                    .each(TaxiFields.BASE_DISTANCE_FIELDS, distanceTupleListener);
        }

        // TODO: enable
        // distanceStream.each(taxiFieldsWithDistance, new StoreInformation(OperatorType.DISTANCE));

        return topology.build();
    }

    public String getTopic() {
        return topic;
    }

    public LocalKafkaInstance getKafkaInstance() {
        return localKafkaInstance;
    }

    private OpaqueTridentKafkaSpout buildKafkaSpout() {
        ZkHosts zkHosts = new ZkHosts(localKafkaInstance.getConnectString());
        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(zkHosts, topic);
        spoutConfig.scheme = new KeyValueSchemeAsMultiScheme(new TaxiEntryKeyValueScheme());

        return new OpaqueTridentKafkaSpout(spoutConfig);
    }

    public void submitLocalCluster() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        startKafka();

        cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, build());
    }

    public static TridentProcessingTopology createWithListeners(BaseFilter speedListener, BaseFilter avgSpeedListener, BaseFilter distanceListener) throws Exception {
        return new TridentProcessingTopology(
                "taxi", "localhost", 6379,
                speedListener, avgSpeedListener, distanceListener);
    }

    public static TridentProcessingTopology createWithTopicAndListeners(String topic, BaseFilter speedListener, BaseFilter avgSpeedListener, BaseFilter distanceListener) throws Exception {
        return new TridentProcessingTopology(
                topic, "localhost", 6379,
                speedListener, avgSpeedListener, distanceListener);
    }

    public static void main(String[] args) throws Exception {
        BaseFilter speedListener = new Debug("speed");
        BaseFilter avgSpeedListener = new Debug("avgSpeed");
        BaseFilter distanceListener = new Debug("distance");

        TridentProcessingTopology topology = createWithListeners(speedListener, avgSpeedListener, distanceListener);
        topology.submitLocalCluster();

        try {
            Thread.sleep(60* 1000);
        } catch (InterruptedException e) {

        }

        topology.stop();
    }
}
