package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TaxiEntryKeyValueScheme;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.StoreInformation;
import at.ac.tuwien.aic.streamprocessing.storm.trident.StoreInformation.OperatorType;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentProcessingTopology {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;

    public static StormTopology buildTopology(String zkConnect) {
        Fields taxiFields = new Fields("id", "timestamp", "latitude", "longitude");
        Fields taxiFieldsWithSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed");
        Fields taxiFieldsWithAvgSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
        Fields taxiFieldsWithDistance = new Fields("id", "timestamp", "latitude", "longitude", "distance");


        TridentTopology topology = new TridentTopology();
        ZkHosts zkHosts = new ZkHosts(zkConnect);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zkHosts, "taxi");
        spoutConf.scheme = new KeyValueSchemeAsMultiScheme(new TaxiEntryKeyValueScheme());
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);

        Stream inputStream = topology.newStream("kafka-spout", spout);

        inputStream.partitionAggregate(taxiFields, new CalculateSpeed(), taxiFieldsWithSpeed).toStream().each(taxiFieldsWithSpeed, new Debug("speed"))
                .partitionAggregate(taxiFieldsWithSpeed, new CalculateAverageSpeed(), taxiFieldsWithAvgSpeed).toStream()
                .each(taxiFieldsWithAvgSpeed, new Debug("avgSpeed")).each(taxiFieldsWithAvgSpeed, new StoreInformation(OperatorType.AVERGAGE_SPEED));

        inputStream.partitionAggregate(taxiFields, new CalculateDistance(), taxiFieldsWithDistance).toStream()
                .each(taxiFieldsWithDistance, new Debug("distance")).each(taxiFieldsWithDistance, new StoreInformation(OperatorType.DISTANCE));


        return topology.build();
    }

    public static LocalKafkaInstance startZookeeperAndKafka(String topic) throws Exception {
        LocalKafkaInstance kafka = new LocalKafkaInstance(9092, 2000);

        kafka.start();
        kafka.createTopic(topic);

        return kafka;
    }

    public static void main(String[] args) throws Exception {
        // this method is for testing only
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        LocalKafkaInstance kafka = startZookeeperAndKafka("taxi");


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, buildTopology(kafka.getConnectString()));

        Thread.sleep(20000);

        cluster.shutdown();
        kafka.stop();

        throw new RuntimeException("Exit Zookeper the Hard way");
    }

}
