package at.ac.tuwien.aic.streamprocessing.storm;

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
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TaxiEntryKeyValueScheme;
import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.CalculateSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.CalculateTaxiCountAndDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.AreaLeavingNotifier;
import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.DrivingTaxiFilter;
import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.PropagateInformation;
import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.PropagateLocation;
import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.SpeedingNotifier;
import at.ac.tuwien.aic.streamprocessing.storm.trident.persist.InfoType;
import at.ac.tuwien.aic.streamprocessing.storm.trident.persist.StoreInformation;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateUpdater;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AvgSpeedQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedStateQuery;
import at.ac.tuwien.aic.streamprocessing.storm.tuple.TaxiFields;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;

public class OptimizedTridentProcessingTopology {
    private final Logger logger = LoggerFactory.getLogger(TridentProcessingTopology.class);

    private static final String SPOUT_ID = "kafka-spout";
    public static final Fields ID = new Fields("id");

    private String topic;

    private final String redisHost;
    private final int redisPort;

    private final String dashbaordAdress;

    private final BaseFilter speedTupleListener;
    private final BaseFilter avgSpeedTupleListener;
    private final BaseFilter distanceTupleListener;

    private LocalKafkaInstance localKafkaInstance;
    private RedisServer localRedisServer;
    private TridentTopology topology;
    private LocalCluster cluster;

    private boolean stopped = false;

    public OptimizedTridentProcessingTopology(String topic, String redisHost, int redisPort, String dashboardAdress) {
        this.topic = topic;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.dashbaordAdress = dashboardAdress;

        this.speedTupleListener = null;
        this.avgSpeedTupleListener = null;
        this.distanceTupleListener = null;
    }

    public OptimizedTridentProcessingTopology(String topic, String redisHost, int redisPort, String dashboardAdress, BaseFilter speedTupleListener,
                                     BaseFilter avgSpeedTupleListener, BaseFilter distanceTupleListener) {
        this.dashbaordAdress = dashboardAdress;
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
            logger.error("Caught exception while starting kafka. Aborting", e);
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
        Stream inputStream = topology.newStream(SPOUT_ID, spout).groupBy(TaxiFields.ID_ONLY_FIELDS).toStream().filter(new DrivingTaxiFilter(dashbaordAdress));

        // notify dashboard of occurring area violations
        inputStream = inputStream.each(TaxiFields.BASE_FIELDS, new AreaLeavingNotifier(dashbaordAdress));

        // propagate location information
        inputStream = inputStream.each(TaxiFields.BASE_FIELDS, new PropagateLocation(dashbaordAdress));

        // setup speed aggregator
        TridentState speed = topology.newStaticState(StateFactory.createSpeedStateFactory(redisHost, redisPort));
        Stream speedStream = inputStream
                .stateQuery( // query the state for each taxi id
                        speed, ID, new SpeedStateQuery(), TaxiFields.SPEED_STATE_FIELDS)
                .partitionAggregate( // batch-process entries
                        TaxiFields.CALCULATE_SPEED_INPUT_FIELDS, new CalculateSpeed(), TaxiFields.CALCULATE_SPEED_OUTPUT_FIELDS)
                .toStream();

        // update the new speed states
        speedStream.partitionPersist(StateFactory.createSpeedStateFactory(redisHost, redisPort), TaxiFields.CALCULATE_SPEED_OUTPUT_FIELDS,
                new StateUpdater<RedisState<SpeedState>>()).newValuesStream();

        if (speedTupleListener != null) {
            speedStream = speedStream.each(TaxiFields.CALCULATE_SPEED_OUTPUT_FIELDS, speedTupleListener);
        }

        // notify dashboard if vehicle is speeding
        speedStream = speedStream.each(TaxiFields.CALCULATE_SPEED_OUTPUT_FIELDS, new SpeedingNotifier(dashbaordAdress));

        // setup average speed aggregator
        TridentState avgSpeed = topology.newStaticState(StateFactory.createAverageSpeedStateFactory(redisHost, redisPort));
        Stream avgSpeedStream = speedStream
                .stateQuery( // query the state for each taxi id
                        avgSpeed, ID, new AvgSpeedQuery(), TaxiFields.AVG_SPEED_STATE_FIELDS)
                .partitionAggregate( // batch-process entries
                        TaxiFields.AVG_SPEED_INPUT_FIELDS, new CalculateAverageSpeed(), TaxiFields.AVG_SPEED_OUTPUT_FIELDS)
                .toStream();

        // update the new average speed states
        avgSpeedStream.partitionPersist(StateFactory.createAverageSpeedStateFactory(redisHost, redisPort), TaxiFields.AVG_SPEED_OUTPUT_FIELDS,
                new StateUpdater<RedisState<AverageSpeedState>>()).newValuesStream();

        if (avgSpeedTupleListener != null) {
            avgSpeedStream = avgSpeedStream.each(TaxiFields.AVG_SPEED_OUTPUT_FIELDS, avgSpeedTupleListener);
        }

        // forward average speed to redis
        avgSpeedStream.each(TaxiFields.AVG_SPEED_OUTPUT_FIELDS, new StoreInformation(InfoType.AVERAGE_SPEED, redisHost, redisPort));

        // setup distance aggregator
        TridentState distance = topology.newStaticState(StateFactory.createDistanceStateFactory(redisHost, redisPort));
        Stream distanceStream = inputStream
                .stateQuery( // query the state for each taxi id
                        distance, ID, new DistanceQuery(), TaxiFields.DISTANCE_STATE_FIELDS)
                .partitionAggregate( // batch-process entries
                        TaxiFields.CALCULATE_DISTANCE_INPUT_FIELDS, new CalculateDistance(), TaxiFields.CALCULATE_DISTANCE_OUTPUT_FIELDS)
                .toStream();

        // update the new distance states
        distanceStream.partitionPersist(StateFactory.createDistanceStateFactory(redisHost, redisPort), TaxiFields.CALCULATE_DISTANCE_OUTPUT_FIELDS,
                new StateUpdater<RedisState<DistanceState>>()).newValuesStream();

        if (distanceTupleListener != null) {
            distanceStream = distanceStream.toStream().each(TaxiFields.CALCULATE_DISTANCE_OUTPUT_FIELDS, distanceTupleListener);
        }

        // forward distance to redis
        Stream dashbaordStream = distanceStream.each(TaxiFields.CALCULATE_DISTANCE_OUTPUT_FIELDS,
                new StoreInformation(InfoType.DISTANCE, redisHost, redisPort));

        // aggregate amount of taxis + overall distance and propagate to dashboard
        dashbaordStream = dashbaordStream.persistentAggregate(new MemoryMapState.Factory(), TaxiFields.INFORMATION_INPUT_FIELDS,
                new CalculateTaxiCountAndDistance(), TaxiFields.INFORMATION_OUTPUT_FIELDS).newValuesStream();
        dashbaordStream.filter(TaxiFields.INFORMATION_OUTPUT_FIELDS, new PropagateInformation(dashbaordAdress));

        return topology.build();
    }

    public String getTopic() {
        return topic;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() {
        return redisPort;
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

    public static OptimizedTridentProcessingTopology createWithListeners(BaseFilter speedListener, BaseFilter avgSpeedListener, BaseFilter distanceListener)
            throws Exception {
        return new OptimizedTridentProcessingTopology("taxi", "localhost", 6379, "http://127.0.0.1:3000", speedListener, avgSpeedListener, distanceListener);
    }

    public static OptimizedTridentProcessingTopology createWithTopicAndListeners(String topic, BaseFilter speedListener, BaseFilter avgSpeedListener,
                                                                        BaseFilter distanceListener) throws Exception {
        return new OptimizedTridentProcessingTopology(topic, "localhost", 6379, "http://127.0.0.1:3000", speedListener, avgSpeedListener, distanceListener);
    }

    public static void main(String[] args) throws Exception {
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

        OptimizedTridentProcessingTopology topology = createWithListeners(speedListener, avgSpeedListener, distanceListener);
        topology.submitLocalCluster();

        Runtime.getRuntime().addShutdownHook(new Thread(topology::stop));
    }
}
