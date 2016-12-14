package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class SpeedQuery extends StateQuery<SpeedDB, Position> {
    @Override
    protected Position getInitial() {
        return new Position();
    }

    @Override
    protected List<Position> query(SpeedDB state, List<Integer> ids) {
        return state.bulkGetLocations(ids);
    }
}
