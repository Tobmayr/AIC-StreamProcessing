package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.speed.Position;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;

public class CalculateSpeed extends LastState<Position> {
    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     *
     */

    private final Logger logger = LoggerFactory.getLogger(CalculateSpeed.class);

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


        Double distance = this.distance(newTuple, previous.latitude, previous.longitude); // in km
        Double time = this.time(previous.timestamp, input.timestamp); //in hours
        Double speed;

        if (Double.compare(time, 0.0) == 0) {
            speed = 0.0;
        } else {
            speed = distance / time;  //in kmh
        }

        collector.emit(new Values(id, input.timestamp, input.latitude, input.longitude, speed, previous));
        logger.debug("(speed): [taxiId={}, timestamp={}, latitude={}, longitude={}, speed={}]", id ,input.timestamp, input.latitude, input.longitude, String.format("%.3f", speed));

        return input.getPosition();
    }

}

