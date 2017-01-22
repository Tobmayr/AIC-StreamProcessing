package at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;

public class SpeedStateQuery extends StateQuery<RedisState<SpeedState>, SpeedState> {
    public SpeedStateQuery() {
        super("speed");
    }
}
