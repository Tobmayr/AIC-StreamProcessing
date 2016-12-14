package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import org.apache.storm.trident.state.State;

public class SpeedDBFactory extends at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory {
    public SpeedDBFactory(String type, String redisHost, int redisPort) {
        super(type, redisHost, redisPort);
    }

    @Override
    protected State create(String type, String redisHost, int redisPort) {
        return new SpeedDB(redisHost, redisPort);
    }
}
