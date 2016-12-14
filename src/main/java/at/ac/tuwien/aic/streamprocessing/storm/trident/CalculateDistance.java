package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.lang.reflect.Array;
import java.time.LocalDateTime;
import java.util.ArrayList;

public class CalculateDistance extends LastState<CalculateDistance.TaxiDistance> {

    class TaxiDistance {

        public TaxiDistance() {
        }

        public TaxiDistance(ArrayList<Double> list) {
            this.latitude = list.get(0);
            this.longitude = list.get(1);
            this.traveled = list.get(2);
        }

        String timestamp;
        Double latitude;
        Double longitude;
        Double traveled;

        @Override
        public String toString() {
            return (super.toString() + timestamp + '_' + latitude + '_' + longitude + '_' + traveled);
        }
    }

    protected TaxiDistance calculate(TridentTuple newTuple, TaxiDistance taxiDistance, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        String timestamp = newTuple.getStringByField("timestamp");

        TaxiDistance td = new TaxiDistance();
        td.latitude = newTuple.getDoubleByField("latitude");
        td.longitude = newTuple.getDoubleByField("longitude");
        td.timestamp = timestamp;

        TaxiDistance persisted = new TaxiDistance((ArrayList<Double>) newTuple.getValueByField("distanceArray"));

        if (taxiDistance == null) { // first time in this batch, use persisted values
            taxiDistance = new TaxiDistance();
            taxiDistance.traveled = persisted.traveled;
            taxiDistance.latitude = persisted.latitude != 0d ? persisted.latitude : td.latitude;
            taxiDistance.longitude = persisted.longitude != 0d ? persisted.longitude : td.longitude;
        }

        Double distance = this.distance(newTuple, taxiDistance.latitude, taxiDistance.longitude); // in km

        td.traveled = taxiDistance.traveled + distance;
        ArrayList<Double> distanceArray = new ArrayList<>();
        distanceArray.add(td.latitude);
        distanceArray.add(td.longitude);
        distanceArray.add(td.traveled);
        collector.emit(new Values(id, timestamp, td.latitude, td.longitude, td.traveled, distanceArray));


        return td;
    }

}
