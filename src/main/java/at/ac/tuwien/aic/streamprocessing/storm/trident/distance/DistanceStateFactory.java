package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class DistanceStateFactory extends StateFactory<DistanceState> {
    public DistanceStateFactory(String redisHost, int redisPort) {
        super("distance", redisHost, redisPort);
    }

    @Override
    protected StateObjectMapper<DistanceState> createMapper() {
        return new DistanceStateMapper();
    }
}
