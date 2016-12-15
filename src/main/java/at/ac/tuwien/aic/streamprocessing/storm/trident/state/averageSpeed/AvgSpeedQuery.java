package at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class AvgSpeedQuery extends StateQuery<RedisState<AverageSpeedState>, AverageSpeedState> {
    public AvgSpeedQuery() {
        super("avgSpeed");
    }
}
