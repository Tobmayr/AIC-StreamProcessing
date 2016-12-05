package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class CalculateAverageSpeed extends LastState<CalculateAverageSpeed.TaxiAvgSpeed> {

    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     */

    class TaxiAvgSpeed {
        String lastTimestamp;
        Double avgSpeed;
        Double hours; // time driving
    }

    protected TaxiAvgSpeed calculate(TridentTuple newTuple, TaxiAvgSpeed oldAvgSpeed, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        Double speed = newTuple.getDoubleByField("speed");
        Double latitude = newTuple.getDoubleByField("latitude");
        Double longitude = newTuple.getDoubleByField("longitude");

        TaxiAvgSpeed newAvgSpeed = new TaxiAvgSpeed();
        newAvgSpeed.lastTimestamp = newTuple.getStringByField("timestamp");

        if (oldAvgSpeed == null) {
            newAvgSpeed.avgSpeed = 0d;
            newAvgSpeed.hours = 0d;
        } else {
            Double time = this.time(oldAvgSpeed.lastTimestamp, newAvgSpeed.lastTimestamp); //in hours
            newAvgSpeed.avgSpeed = (oldAvgSpeed.avgSpeed*oldAvgSpeed.hours + speed*time) / (oldAvgSpeed.hours + time);
            newAvgSpeed.hours = oldAvgSpeed.hours + time;

            collector.emit(new Values(id, newAvgSpeed.lastTimestamp, latitude, longitude, speed, newAvgSpeed.avgSpeed));
        }

        return newAvgSpeed;
    }



}

