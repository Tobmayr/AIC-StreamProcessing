package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class SpeedDBFactory implements StateFactory {
    private final String redisHost;
    private final int redisPort;
    private String type;

    public SpeedDBFactory (String type, String redisHost, int redisPort) {
        this.type = type;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }
    public State makeState(Map conf, IMetricsContext con, int partitionIndex, int numPartitions) {
        return new SpeedDB(type,redisHost,redisPort);
    }
}
