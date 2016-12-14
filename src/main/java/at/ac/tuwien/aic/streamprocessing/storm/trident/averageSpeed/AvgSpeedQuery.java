package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class AvgSpeedQuery extends StateQuery<AvgSpeedDB, AvgSpeed> {

    @Override
    protected AvgSpeed getInitial() {
        return new AvgSpeed();
    }

    @Override
    protected List<AvgSpeed> query(AvgSpeedDB state, List<Integer> ids) {
        return state.getAll(ids);
    }
}
