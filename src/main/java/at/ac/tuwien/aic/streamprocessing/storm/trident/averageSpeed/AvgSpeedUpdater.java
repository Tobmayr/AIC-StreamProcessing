package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class AvgSpeedUpdater extends BaseStateUpdater<AvgSpeedDB> {
    public void updateState(AvgSpeedDB state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Integer> ids = new ArrayList<>();
        List<AvgSpeed> locations = new ArrayList<>();
        for(TridentTuple t: tuples) {
            if (t != null) {
                ids.add(t.getInteger(0));
                locations.add((AvgSpeed) t.getValueByField("avgSpeedObject"));
            } else {
                ids.add(null);
                locations.add(null);
            }
        }
        state.setLocationsBulk(ids, locations);
    }
}
