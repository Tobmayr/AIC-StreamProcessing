package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;

import static at.ac.tuwien.aic.streamprocessing.storm.trident.Haversine.haversine;

public abstract class LastState<T> extends BaseAggregator<LocationMapState<T>> {

    abstract T calculate(TridentTuple t1, T oldMap, TridentCollector collector);

    public Double distance(TridentTuple t1, Double oldLatitude, Double oldLongitude) {
        Double latitude = t1.getDoubleByField("latitude");
        Double longitude = t1.getDoubleByField("longitude");

        Double lastLatitude = oldLatitude;
        Double lastLongitude = oldLongitude;

        return haversine(lastLatitude, lastLongitude, latitude, longitude);
    }

    public Double time(String startTimestamp, String endTimestamp) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime startTime = fmt.parseDateTime(startTimestamp);
        DateTime endTime = fmt.parseDateTime(endTimestamp);
        return (new Duration(startTime, endTime)).getMillis() / 3600000.0;
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
