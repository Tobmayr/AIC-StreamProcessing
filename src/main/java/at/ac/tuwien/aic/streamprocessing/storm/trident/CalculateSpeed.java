package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.speed.Position;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class CalculateSpeed extends LastState<Position> {

    class PositionTuple {
        Double latitude;
        Double longitude;
        String timestamp;
        LocalDateTime ts;
        Position persisted;

        PositionTuple(TridentTuple tuple) {
            this.timestamp = tuple.getStringByField("timestamp");
            this.ts = Timestamp.parse(this.timestamp);
            this.latitude = tuple.getDoubleByField("latitude");
            this.longitude = tuple.getDoubleByField("longitude");
            this.persisted = (Position) tuple.getValueByField("positionArray");
        }

        Position getPosition () {
            Position position = new Position();
            position.timestamp = this.timestamp;
            position.latitude = this.latitude;
            position.longitude = this.longitude;
            return position;
        }
    }

    protected Position calculate(TridentTuple newTuple, Position previous, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        PositionTuple input = new PositionTuple(newTuple);

        if (previous == null && input.persisted != null) {
            previous = input.persisted;
        }

        if (previous == null && input.persisted == null) {
            previous = input.getPosition();
        }

        // act like there has already been a tuple
        previous.timestamp = previous.timestamp == null ? input.timestamp : previous.timestamp;
        previous.latitude = previous.latitude == null ? input.latitude : previous.latitude;
        previous.longitude = previous.longitude == null ? input.longitude : previous.longitude;

        LocalDateTime oldTime = Timestamp.parse(previous.timestamp);
        LocalDateTime newTime = Timestamp.parse(input.timestamp);

            Double speed;

            if (oldTime.isAfter(newTime) || oldTime.isEqual(newTime)) {
//                System.out.println("Old tuple is not older than new one!");

                // since it is not meaningful to compute the speed in this case, just use a default value of 0.0
                speed = 0.0;
            } else {
                Double distance = this.distance(newTuple, previous.latitude, previous.longitude); // in km
                Double time = this.time(previous.timestamp, input.timestamp); //in hours
                speed = distance / time;  //in kmh
            }

            collector.emit(new Values(id, input.timestamp, input.latitude, input.longitude, speed, previous));


        return input.getPosition();
    }

}

