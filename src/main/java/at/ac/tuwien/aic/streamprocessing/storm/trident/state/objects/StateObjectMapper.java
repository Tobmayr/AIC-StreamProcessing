package at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public interface StateObjectMapper<T extends StateObject> {
    Values toStateTuple(T state);

    Values createInitialStateTuple();


    T fromTuple(TridentTuple tuple);

    Values toTuple(Integer id, T state);

    T parseState(TridentTuple tuple);

    // redis (de)serialization
    String serializeToRedis(T state);
    T deserializeFromRedis(String value);
}
