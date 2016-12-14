package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class QueryDistance extends BaseQueryFunction<DistanceDB, ArrayList<Double>> {
    public List<ArrayList<Double>> batchRetrieve(DistanceDB state, List<TridentTuple> inputs) {
        List<Integer> userIds = new ArrayList<>();
        for(TridentTuple input: inputs) {
            System.out.println(input);
            userIds.add(input.getIntegerByField("id"));
        }
        return state.bulkGetLocations(userIds);
    }

    public void execute(TridentTuple tuple, ArrayList<Double> location, TridentCollector collector) {
        if(location != null) {
            collector.emit(new Values(location));
        } else {
             ArrayList<Double> loco = new ArrayList<>();
            loco.add(0d);
            loco.add(0d);
            loco.add(0d);
            collector.emit(new Values(loco));
        }
    }
}
