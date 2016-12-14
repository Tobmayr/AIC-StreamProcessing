package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseStateUpdater;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class SpeedUpdater extends BaseStateUpdater<SpeedDB> {
    public void updateState(SpeedDB state, List<TridentTuple> tuples, TridentCollector collector) {
        List<Integer> ids = new ArrayList<>();
        List<Position> locations = new ArrayList<>();
        for(TridentTuple t: tuples) {
            if (t != null) {
                ids.add(t.getInteger(0));
                locations.add((Position) t.getValueByField("positionArray"));
            } else {
                ids.add(null);
                locations.add(null);
            }
        }
        state.setLocationsBulk(ids, locations);
    }
}
