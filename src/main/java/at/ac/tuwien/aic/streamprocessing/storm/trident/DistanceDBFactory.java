package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class DistanceDBFactory implements StateFactory {
    private final String redisHost;
    private final int redisPort;
    private String type;

    public DistanceDBFactory (String type, String redisHost, int redisPort) {
        this.type = type;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }
    public State makeState(Map conf, IMetricsContext con, int partitionIndex, int numPartitions) {
        return new DistanceDB(type,redisHost,redisPort);
    }
}
