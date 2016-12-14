package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateQuery;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.state.BaseQueryFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

public class QueryDistance extends StateQuery<DistanceDB, ArrayList<Double>> {
    @Override
    protected ArrayList<Double> getInitial() {
        ArrayList<Double> loco = new ArrayList<>();
        loco.add(0d);
        loco.add(0d);
        loco.add(0d);

        return loco;
    }

    @Override
    protected List<ArrayList<Double>> query(DistanceDB state, List<Integer> ids) {
        return state.getAll(ids);
    }
}
