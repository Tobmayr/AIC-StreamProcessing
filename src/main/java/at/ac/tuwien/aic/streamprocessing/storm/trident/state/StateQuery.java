package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapperFactory;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class StateQuery<ST extends RedisState<T>, T extends StateObject> extends BaseQueryFunction<ST, T> {
    private final String type;
    private StateObjectMapper<T> mapper;

    public StateQuery(String type) {
        this.type = type;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        StateObjectMapperFactory factory = new StateObjectMapperFactory(type);
        mapper = factory.create();
    }

    @Override
    public List<T> batchRetrieve(ST state, List<TridentTuple> args) {
        List<Integer> ids = args.stream()
                .map(t -> t.getIntegerByField("id"))
                .collect(Collectors.toList());

        return state.getAll(ids);
    }

    @Override
    public void execute(TridentTuple tuple, T result, TridentCollector collector) {
        if (result == null) {
            // no state yet, just emit initial value
            collector.emit(mapper.createInitialStateTuple());
        } else {
            // just emit the produced value
            collector.emit(mapper.toStateTuple(result));
        }
    }
}
