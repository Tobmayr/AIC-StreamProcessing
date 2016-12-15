package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapperFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedState;
import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;

import java.util.Map;

public class StateFactory<T extends StateObject> implements org.apache.storm.trident.state.StateFactory {
    private final String type;
    private final String redisHost;
    private final int redisPort;

    public StateFactory(String type, String redisHost, int redisPort) {
        this.type = type;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        return create();
    }

    public RedisState<T> create() {
        return new RedisState<T>(type, redisHost, redisPort, createMapper());
    }

    private StateObjectMapper<T> createMapper() {
        StateObjectMapperFactory factory = new StateObjectMapperFactory(type);
        return factory.create();
    }

    public static StateFactory<SpeedState> createSpeedStateFactory(String redisHost, int redisPort) {
        return new StateFactory<>("speed", redisHost, redisPort);
    }

    public static StateFactory<AverageSpeedState> createAverageSpeedStateFactory(String redisHost, int redisPort) {
        return new StateFactory<>("avgSpeed", redisHost, redisPort);
    }

    public static StateFactory<DistanceState> createDistanceStateFactory(String redisHost, int redisPort) {
        return new StateFactory<>("distance", redisHost, redisPort);
    }
}
