package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

public class StateUpdater<ST extends RedisState> extends BaseStateUpdater<ST> {

    @Override
    public void updateState(ST state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Integer> ids = tuples.stream().filter(t -> t != null).map(t -> t.getIntegerByField("id")).collect(Collectors.toList());

        state.setAll(ids, state.transformTuples(tuples));
    }
}
