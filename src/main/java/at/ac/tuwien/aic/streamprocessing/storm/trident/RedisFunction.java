package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class RedisFunction extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        // TODO redis tuple save
        return false;
    }
}
