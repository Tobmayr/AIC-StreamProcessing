package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.time.LocalDateTime;

public class CalculateSpeed extends LastState<CalculateSpeed.Position> {

    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     *
     */

    class Position {
        String timestamp;
        Double latitude;
        Double longitude;
    }

    protected Position calculate(TridentTuple newTuple, Position position, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        Position newPosition = new Position();
        newPosition.timestamp = newTuple.getStringByField("timestamp");
        newPosition.latitude = newTuple.getDoubleByField("latitude");
        newPosition.longitude = newTuple.getDoubleByField("longitude");

        if (position != null) {
            LocalDateTime oldTime = Timestamp.parse(position.timestamp);
            LocalDateTime newTime = Timestamp.parse(newPosition.timestamp);

            Double speed;

            if (oldTime.isAfter(newTime) || oldTime.isEqual(newTime)) {
                System.out.println("Old tuple is not older than new one!");

                // since it is not meaningful to compute the speed in this case, just use a default value of 0.0
                speed = 0.0;
            } else {
                Double distance = this.distance(newTuple, position.latitude, position.longitude); // in km
                Double time = this.time(position.timestamp, newPosition.timestamp); //in hours
                speed = distance / time;  //in kmh
            }

            collector.emit(new Values(id, newPosition.timestamp, newPosition.latitude, newPosition.longitude, speed));
        }

        return newPosition;
    }

}

