package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;

import java.util.Map;

public abstract class StateFactory implements org.apache.storm.trident.state.StateFactory {
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
        return create(type, redisHost, redisPort);
    }

    protected abstract State create(String type, String redisHost, int redisPort);
}
