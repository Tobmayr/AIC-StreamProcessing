package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import org.apache.storm.trident.state.State;

public class AvgSpeedDBFactory extends at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory {
    public AvgSpeedDBFactory(String type, String redisHost, int redisPort) {
        super(type, redisHost, redisPort);
    }

    @Override
    protected State create(String type, String redisHost, int redisPort) {
        return new AvgSpeedDB(redisHost, redisPort);
    }
}
