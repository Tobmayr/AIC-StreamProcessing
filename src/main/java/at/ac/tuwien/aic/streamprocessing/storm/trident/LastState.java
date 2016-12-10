package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

import static at.ac.tuwien.aic.streamprocessing.storm.trident.Haversine.haversine;

public abstract class LastState<T> extends BaseAggregator<LocationMapState<T>> {

    abstract T calculate(TridentTuple t1, T oldMap, TridentCollector collector);

    public Double distance(TridentTuple t1, Double oldLatitude, Double oldLongitude) {
        Double latitude = t1.getDoubleByField("latitude");
        Double longitude = t1.getDoubleByField("longitude");

        return haversine(oldLatitude, oldLongitude, latitude, longitude);
    }

    public Double time(String startTimestamp, String endTimestamp) {
        LocalDateTime startTime = Timestamp.parse(startTimestamp);
        LocalDateTime endTime = Timestamp.parse(endTimestamp);

        return ChronoUnit.MILLIS.between(startTime, endTime) / (60. * 60.0 * 1000.0);
    }

    public LocationMapState<T> init(Object batchId, TridentCollector collector) {
        return new LocationMapState<T>();
    }

    public void aggregate(LocationMapState<T> state, TridentTuple newTuple, TridentCollector collector) {
        HashMap<Integer, T> map = state.map;
        Integer id = newTuple.getIntegerByField("id");

        T oldMap = map.get(id); // get previous state
        T newMap = calculate(newTuple, oldMap, collector);
        map.put(id, newMap); // update previous state
    }

    public void complete(LocationMapState state, TridentCollector collector) {
        // nothing to do here
    }
}

class LocationMapState<T> {
    HashMap<Integer, T> map = new HashMap<>();
}
