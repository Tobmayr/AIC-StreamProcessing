package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import org.apache.storm.task.IMetricsContext;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.state.StateFactory;

import java.util.Map;

public class AvgSpeedDBFactory implements StateFactory {
    private final String redisHost;
    private final int redisPort;
    private String type;

    public AvgSpeedDBFactory (String type, String redisHost, int redisPort) {
        this.type = type;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }
    public State makeState(Map conf, IMetricsContext con, int partitionIndex, int numPartitions) {
        return new AvgSpeedDB(type,redisHost,redisPort);
    }
}
