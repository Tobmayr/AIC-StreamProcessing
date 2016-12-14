package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TaxiEntryKeyValueScheme;
import at.ac.tuwien.aic.streamprocessing.storm.trident.*;
import at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed.AvgSpeedDBFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed.AvgSpeedQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed.AvgSpeedUpdater;
import at.ac.tuwien.aic.streamprocessing.storm.trident.distance.DistanceDBFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.distance.DistanceUpdater;
import at.ac.tuwien.aic.streamprocessing.storm.trident.distance.QueryDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.speed.SpeedDBFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.speed.SpeedQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.speed.SpeedUpdater;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Peeker;
import at.ac.tuwien.aic.streamprocessing.storm.tuple.TaxiFields;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.builtin.Debug;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

public class TridentProcessingTopology {
    private final Logger logger = LoggerFactory.getLogger(TridentProcessingTopology.class);

    private static final String SPOUT_ID = "kafka-spout";
    public static final Fields BASE_DIST_DISTARR = new Fields("id", "timestamp", "latitude", "longitude", "distance", "distanceArray");
    public static final Fields ID = new Fields("id");
    public static final Fields DISTANCEARRAY = new Fields("distanceArray");
    public static final Fields ID_DISTARR = new Fields("id", "distanceArray");

    private String topic;

    private final String redisHost;
    private final int redisPort;

    private final BaseFilter speedTupleListener;
    private final BaseFilter avgSpeedTupleListener;
    private final BaseFilter distanceTupleListener;

    private LocalKafkaInstance localKafkaInstance;
    private RedisServer localRedisServer;
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

    public TridentProcessingTopology(String topic, String redisHost, int redisPort, BaseFilter speedTupleListener, BaseFilter avgSpeedTupleListener,
            BaseFilter distanceTupleListener) {
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
            cleanUpRedis();
            cluster.shutdown();
            stopKafka();
            stopRedisServer();
        } catch (Exception e) {
            logger.error("Failed to stop cluster.", e);
            System.exit(1);
        }
    }

    private void stopRedisServer() {
        localRedisServer.stop();
    }

    private void startRedis() {
        try {
            localRedisServer = new RedisServer(redisPort);
            localRedisServer.start();
        } catch (Exception e) {
            logger.error("Caught exception while starting redis. Aborting", e);
            System.exit(1);
        }
    }

    private void cleanUpRedis() {
        try {
            Jedis jedis = new Jedis(redisHost, redisPort);
            jedis.connect();
            jedis.flushDB();
            jedis.disconnect();
            jedis.close();
        } catch (Exception e) {
            logger.error("Caught exception while cleaning up redis database. Ignoring", e);
        }
    }

    private void startKafka() {
        localKafkaInstance = new LocalKafkaInstance(9092, 2000);

        try {
            localKafkaInstance.start();
        } catch (Exception e) {
            logger.error("Caught exception while starting kafka. Aborting",e);
            System.exit(1);
        }

        localKafkaInstance.createTopic(topic);
    }

    private void stopKafka() {
        try {
            localKafkaInstance.stop();
        } catch (Exception e) {
            logger.error("Caught exception while stopping kafka. Ignoring.", e);
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
        //Stream speedStream = inputStream.partitionAggregate(TaxiFields.BASE_FIELDS, new CalculateSpeed(), TaxiFields.BASE_SPEED_FIELDS).toStream();
        TridentState speed = topology.newStaticState(new SpeedDBFactory("speed",redisHost,redisPort));
        Stream speedStream = inputStream
                .stateQuery(speed, ID, new SpeedQuery(), new Fields("positionArray"))
//                .peek(new Peeker("speedBefore"))
                .partitionAggregate( TaxiFields.BASE_POSITIONARRAY,  new CalculateSpeed(), TaxiFields.BASE_SPEED_POSITIONARRAY)
                .toStream()
//                .peek(new Peeker("speedAfter"))
                ;
        speedStream.partitionPersist( new SpeedDBFactory("speed",redisHost,redisPort), TaxiFields.ID_POSITIONARRAY, new SpeedUpdater()).newValuesStream(); //.peek(new Peeker("Persistence"));

        if (speedTupleListener != null) {
            speedStream = speedStream.each(TaxiFields.BASE_SPEED_FIELDS, speedTupleListener);
        }

        // setup average speed aggregator
        //speedStream = speedStream.partitionAggregate(TaxiFields.BASE_SPEED_FIELDS, new CalculateAverageSpeed(), TaxiFields.BASE_SPEED_AVG_FIELDS).toStream();
        TridentState avgSpeed = topology.newStaticState(new AvgSpeedDBFactory("avgSpeed",redisHost,redisPort));
        Stream avgSpeedStream = speedStream
                .stateQuery(avgSpeed, ID, new AvgSpeedQuery(), new Fields("avgSpeedObject"))
//                .peek(new Peeker("avgSpeedBefore"))
                .partitionAggregate( TaxiFields.BASE_SPEED_AVGSPEEDOBJECT,  new CalculateAverageSpeed(), TaxiFields.BASE_SPEED_AVG_AVGSPEEDOBJECT)
                .toStream()
//                .peek(new Peeker("avgSpeedAfter"))
                ;
        avgSpeedStream.partitionPersist( new AvgSpeedDBFactory("avgSpeed",redisHost,redisPort), TaxiFields.ID_AVGSPEEDOBJECT, new AvgSpeedUpdater()).newValuesStream(); //.peek(new Peeker("Persistence"));


        if (avgSpeedTupleListener != null) {
            avgSpeedStream = avgSpeedStream.each(TaxiFields.BASE_SPEED_AVG_FIELDS, avgSpeedTupleListener);
        }

        avgSpeedStream.each(TaxiFields.BASE_SPEED_AVG_FIELDS, new StoreInformation(InfoType.AVERAGE_SPEED, redisHost, redisPort));


        // setup distance aggregator
        TridentState distance = topology.newStaticState(new DistanceDBFactory("distance",redisHost,redisPort));
        Stream distanceStream = inputStream
                .stateQuery(distance, ID, new QueryDistance(), DISTANCEARRAY)
//                .peek(new Peeker("distanceBefore"))
                .partitionAggregate( TaxiFields.BASE_DISTARRAY,  new CalculateDistance(), BASE_DIST_DISTARR)
                .toStream()
//                .peek(new Peeker("distanceAfter"))
                ;
        distanceStream.partitionPersist( new DistanceDBFactory("distance",redisHost,redisPort), ID_DISTARR, new DistanceUpdater()).newValuesStream();//.peek(new Peeker("Persistence"));

        if (distanceTupleListener != null) {
            distanceStream = distanceStream.toStream().each(TaxiFields.BASE_DISTANCE_FIELDS, distanceTupleListener);
        }

        distanceStream.each(TaxiFields.BASE_DISTANCE_FIELDS, new StoreInformation(InfoType.DISTANCE, redisHost, redisPort));

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
        startRedis();

        cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, build());
    }

    public static TridentProcessingTopology createWithListeners(BaseFilter speedListener, BaseFilter avgSpeedListener, BaseFilter distanceListener)
            throws Exception {
        return new TridentProcessingTopology("taxi", "localhost", 6379, speedListener, avgSpeedListener, distanceListener);
    }

    public static TridentProcessingTopology createWithTopicAndListeners(String topic, BaseFilter speedListener, BaseFilter avgSpeedListener,
            BaseFilter distanceListener) throws Exception {
        return new TridentProcessingTopology(topic, "localhost", 6379, speedListener, avgSpeedListener, distanceListener);
    }

    public static void main(String[] args) throws Exception {
//        BaseFilter speedListener = new Debug("speed");
//        BaseFilter avgSpeedListener = new Debug("avgSpeed");
//        BaseFilter distanceListener = new Debug("distance");

        BaseFilter speedListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };

        BaseFilter avgSpeedListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };

        BaseFilter distanceListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };

        TridentProcessingTopology topology = createWithListeners(speedListener, avgSpeedListener, distanceListener);
        topology.submitLocalCluster();

        Runtime.getRuntime().addShutdownHook(new Thread(topology::stop));
    }
}
