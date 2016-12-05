package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.storm.redis.AverageSpeedStoreMapper;
import at.ac.tuwien.aic.streamprocessing.storm.redis.DistanceStoreMapper;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TestTaxiFixedDataSpout;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.RedisFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.trident.state.RedisState;
import org.apache.storm.redis.trident.state.RedisStateQuerier;
import org.apache.storm.redis.trident.state.RedisStateUpdater;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentProcessingTopology {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;

    public static StormTopology buildTopology() {
        Fields taxiFields = new Fields("id", "timestamp", "latitude", "longitude");
        Fields taxiFieldsWithSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed");
        Fields taxiFieldsWithAvgSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
        Fields taxiFieldsWithDistance = new Fields("id", "timestamp", "latitude", "longitude", "distance");

        // Setup for redis
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder().setHost(REDIS_HOST).setPort(REDIS_PORT).build();
        AverageSpeedStoreMapper averageSpeedStore = new AverageSpeedStoreMapper();
        DistanceStoreMapper distanceStore = new DistanceStoreMapper();
        RedisState.Factory factory = new RedisState.Factory(poolConfig);

        TridentTopology topology = new TridentTopology();
        TestTaxiFixedDataSpout spout = new TestTaxiFixedDataSpout();
        Stream inputStream = topology.newStream("taxi", spout);
        inputStream.partitionAggregate(taxiFields, new CalculateSpeed(), taxiFieldsWithSpeed).toStream().each(taxiFieldsWithSpeed, new Debug("speed"))
                .partitionAggregate(taxiFieldsWithSpeed, new CalculateAverageSpeed(), taxiFieldsWithAvgSpeed).toStream()
                .each(taxiFieldsWithSpeed, new Debug("avgSpeed"))
                .partitionPersist(factory, taxiFieldsWithAvgSpeed, new RedisStateUpdater(averageSpeedStore).withExpire(86400000), new Fields());

        inputStream.partitionAggregate(taxiFields, new CalculateDistance(), taxiFieldsWithDistance).toStream()
                .each(taxiFieldsWithDistance, new Debug("distance"))
                .partitionPersist(factory, taxiFieldsWithDistance, new RedisStateUpdater(distanceStore).withExpire(86400000), new Fields());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        // this method is for testing only
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, buildTopology());

        Thread.sleep(20000);

        cluster.shutdown();

        throw new RuntimeException("Exit Zookeper the Hard way");
    }

}
