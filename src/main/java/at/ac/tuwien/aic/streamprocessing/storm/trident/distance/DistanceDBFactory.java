package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import org.apache.storm.trident.state.State;

public class DistanceDBFactory extends at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory {
    public DistanceDBFactory(String type, String redisHost, int redisPort) {
        super(type, redisHost, redisPort);
    }

    @Override
    protected State create(String type, String redisHost, int redisPort) {
        return new DistanceDB(type, redisHost, redisPort);
    }
}
