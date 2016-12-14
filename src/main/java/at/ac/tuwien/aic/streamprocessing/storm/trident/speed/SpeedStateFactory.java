package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.SpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.SpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class SpeedStateFactory extends StateFactory<SpeedState> {
    public SpeedStateFactory(String redisHost, int redisPort) {
        super("speed", redisHost, redisPort);
    }

    @Override
    protected StateObjectMapper<SpeedState> createMapper() {
        return new SpeedStateMapper();
    }
}
