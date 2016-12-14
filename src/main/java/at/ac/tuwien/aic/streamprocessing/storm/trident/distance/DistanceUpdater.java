package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class DistanceUpdater extends BaseStateUpdater<DistanceDB> {
    public void updateState(DistanceDB state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Integer> ids = new ArrayList<>();
        List<ArrayList<Double>> locations = new ArrayList<>();
        for(TridentTuple t: tuples) {
            ids.add(t.getInteger(0));
            locations.add( (ArrayList<Double>) t.getValueByField("distanceArray"));
        }
        state.setAll(ids, locations);
    }
}
