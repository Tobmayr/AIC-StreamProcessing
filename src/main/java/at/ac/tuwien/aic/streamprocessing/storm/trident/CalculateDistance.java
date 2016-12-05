package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class CalculateDistance extends LastState<CalculateDistance.TaxiDistance>{

    class TaxiDistance {
        Double latitude;
        Double longitude;
        Double traveled;
    }

    protected TaxiDistance calculate(TridentTuple newTuple, TaxiDistance taxiDistance, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        String timestamp = newTuple.getStringByField("timestamp");

        TaxiDistance td = new TaxiDistance();
        td.latitude = newTuple.getDoubleByField("latitude");
        td.longitude = newTuple.getDoubleByField("longitude");

        if (taxiDistance == null) {
            td.traveled = 0d;
        } else {
            Double distance = this.distance(newTuple, taxiDistance.latitude, taxiDistance.longitude); // in km
            td.traveled = taxiDistance.traveled + distance;
            collector.emit(new Values(id, timestamp, td.latitude, td.longitude, td.traveled));
        }

        return td;
    }

}
