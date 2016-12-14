package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.AverageSpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class AvgSpeedStateFactory extends StateFactory<AverageSpeedState> {
    public AvgSpeedStateFactory(String redisHost, int redisPort) {
        super("avgSpeed", redisHost, redisPort);
    }

    @Override
    protected StateObjectMapper<AverageSpeedState> createMapper() {
        return new AverageSpeedStateMapper();
    }
}
