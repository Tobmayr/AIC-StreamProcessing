package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class DistanceQuery extends StateQuery<RedisState<DistanceState>, DistanceState> {

    @Override
    protected StateObjectMapper<DistanceState> createMapper() {
        return new DistanceStateMapper();
    }
}
