package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;

import static at.ac.tuwien.aic.streamprocessing.storm.trident.Haversine.haversine;

public class CalculateSpeed extends BaseAggregator {

    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     */

    private void speed(TridentTuple input, TridentTuple t2, TridentCollector collector) {
        Integer id = input.getIntegerByField("id"); //test
        String timestamp = input.getStringByField("timestamp");
        Double latitude = input.getDoubleByField("latitude");
        Double longitude = input.getDoubleByField("longitude");

        if (t2 == null) {
            return;
        }
        String lastTimestamp = t2.getStringByField("timestamp");
        Double lastLatitude = t2.getDoubleByField("latitude");
        Double lastLongitude = t2.getDoubleByField("longitude");

        Double speed;// in kmh
        Double distance; // in km
        Double time; // in hours

        if (id == null || timestamp == null || latitude == null || longitude == null) {
            return;
        }

        distance = haversine(lastLatitude, lastLongitude, latitude, longitude);
        time = time(lastTimestamp, timestamp);
        speed = distance / time;

        collector.emit(new Values(id, speed));
    }

    private Double time(String startTimestamp, String endTimestamp) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime startTime = fmt.parseDateTime(startTimestamp);
        DateTime endTime = fmt.parseDateTime(endTimestamp);
        return (new Duration(startTime, endTime)).getMillis() / 3600000.0;
    }

    @Override
    public Object init(Object batchId, TridentCollector collector) {
        return new HashMap<Integer, TridentTuple>();
    }

    @Override
    public void aggregate(Object curr, TridentTuple tuple, TridentCollector collector) {
        HashMap<Integer, TridentTuple> state = (HashMap<Integer, TridentTuple>) curr;
        Integer id = tuple.getIntegerByField("id");

        TridentTuple oldTuple = state.get(id); // get previous state
        speed(tuple, oldTuple, collector); // calculate and emit speed
        state.put(id, tuple); // update previous state

    }

    @Override
    public void complete(Object state, TridentCollector collector) {
        // nothing to do here
    }
}
