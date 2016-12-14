package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class SpeedQuery extends BaseQueryFunction<SpeedDB, Position> {
    public List<Position> batchRetrieve(SpeedDB state, List<TridentTuple> inputs) {
        List<Integer> userIds = new ArrayList<>();
        for(TridentTuple input: inputs) {
            userIds.add(input.getIntegerByField("id"));
        }
        return state.bulkGetLocations(userIds);
    }

    public void execute(TridentTuple tuple, Position location, TridentCollector collector) {
        if(location != null) {
            collector.emit(new Values(location));
        } else {
            collector.emit(new Values(new Position()));
        }
    }
}
