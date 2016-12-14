package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed.AvgSpeed;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.time.LocalDateTime;

public class CalculateAverageSpeed extends LastState<AvgSpeed> {

    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     */


    protected AvgSpeed calculate(TridentTuple newTuple, AvgSpeed oldAvgSpeed, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        Double speed = newTuple.getDoubleByField("speed");
        Double latitude = newTuple.getDoubleByField("latitude");
        Double longitude = newTuple.getDoubleByField("longitude");
        AvgSpeed persisted = (AvgSpeed) newTuple.getValueByField("avgSpeedObject");

        AvgSpeed newAvgSpeed = new AvgSpeed();
        newAvgSpeed.lastTimestamp = newTuple.getStringByField("timestamp");

        if (oldAvgSpeed == null) {
            // this happens at the start of a new batch
            oldAvgSpeed = persisted;
        }

        // act like there has already been a tuple
        oldAvgSpeed.lastTimestamp = oldAvgSpeed.lastTimestamp == null ? newAvgSpeed.lastTimestamp : oldAvgSpeed.lastTimestamp;
        oldAvgSpeed.avgSpeed = oldAvgSpeed.avgSpeed == null ? 0d : oldAvgSpeed.avgSpeed;
        oldAvgSpeed.hours = oldAvgSpeed.hours == null ? 0d : oldAvgSpeed.hours;


        if (oldAvgSpeed == null) {
            newAvgSpeed.avgSpeed = 0d;
            newAvgSpeed.hours = 0d;
        } else {
            LocalDateTime oldTime = Timestamp.parse(oldAvgSpeed.lastTimestamp);
            LocalDateTime newTime = Timestamp.parse(newAvgSpeed.lastTimestamp);

            Double time;

            if (oldTime.isAfter(newTime) || oldTime.isEqual(newTime)) {
//                System.out.println("Old tuple is not older than new one!");

                // since it is not meaningful to compute the time in this case, just use a default value of 0.0
                time = 0.0;
            } else {
                time = this.time(oldAvgSpeed.lastTimestamp, newAvgSpeed.lastTimestamp); //in hours
            }

            newAvgSpeed.hours = oldAvgSpeed.hours + time;

            if (Double.compare(newAvgSpeed.hours, 0.0) == 0) {
                newAvgSpeed.avgSpeed = 0.0;
            } else {
                newAvgSpeed.avgSpeed = (oldAvgSpeed.avgSpeed*oldAvgSpeed.hours + speed*time) / newAvgSpeed.hours;
            }

            collector.emit(new Values(id, newAvgSpeed.lastTimestamp, latitude, longitude, speed, newAvgSpeed.avgSpeed,newAvgSpeed));
        }

        return newAvgSpeed;
    }



}

