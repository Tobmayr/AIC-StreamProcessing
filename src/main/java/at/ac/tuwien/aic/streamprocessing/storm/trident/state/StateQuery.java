package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.stream.Collectors;

public abstract class StateQuery<ST extends State, T> extends BaseQueryFunction<ST, T> {

    @Override
    public List<T> batchRetrieve(ST state, List<TridentTuple> args) {
        List<Integer> ids = args.stream()
                .map(t -> t.getIntegerByField("id"))
                .collect(Collectors.toList());

        return query(state, ids);
    }

    @Override
    public void execute(TridentTuple tuple, T result, TridentCollector collector) {
        if (result == null) {
            // no state yet, just emit initial value
            // TODO: suppress emit?
            collector.emit(new Values(getInitial()));
        } else {
            // just emit the produced value
            collector.emit(new Values(result));
        }
    }

    protected abstract T getInitial();
    protected abstract List<T> query(ST state, List<Integer> ids);
}
