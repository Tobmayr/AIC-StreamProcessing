package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public abstract class Aggregator<T extends StateObject> extends BaseAggregator<Map<Integer, T>> {

    @Override
    public Map<Integer, T> init(Object batchId, TridentCollector collector) {
        return new HashMap<>();
    }

    @Override
    public void aggregate(Map<Integer, T> batchState, TridentTuple tuple, TridentCollector collector) {
        Integer id = tuple.getIntegerByField("id");

        // lookup previous state in this batch
        T previous = batchState.get(id);

        if (previous == null) {
            // first tuple for given id in this batch
            // check if there was a previous state
            boolean hasState = tuple.getBooleanByField("has_state");
            if (hasState) {
                // tuple carries previous state, parse it
                previous = getMapper().parseState(tuple);
            } else {
                // tuple has no state attached, just project actual fields
                previous = getMapper().fromTuple(tuple);
            }
        }

        // compute current state
        T next = compute(previous, tuple);
        batchState.put(id, next);

        // emit corresponding tuple
        Values resultTuple = getMapper().toTuple(id, next);
        collector.emit(resultTuple);
    }

    protected abstract StateObjectMapper<T> getMapper();

    protected abstract T compute(T previous, TridentTuple tuple);

    @Override
    public void complete(Map<Integer, T> val, TridentCollector collector) {
        // nothing to do
    }
}
