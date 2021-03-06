package at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;

public class DistanceQuery extends StateQuery<RedisState<DistanceState>, DistanceState> {
    public DistanceQuery() {
        super("distance");
    }
}
