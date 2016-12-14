package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.AverageSpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class AvgSpeedQuery extends StateQuery<RedisState<AverageSpeedState>, AverageSpeedState> {

    @Override
    protected StateObjectMapper<AverageSpeedState> createMapper() {
        return new AverageSpeedStateMapper();
    }
}