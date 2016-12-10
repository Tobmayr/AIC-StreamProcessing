package at.ac.tuwien.aic.streamprocessing.trident.utils;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class SpeedTupleListener extends TridentTupleListener<SpeedTupleListener.SpeedTuple> {

    public SpeedTupleListener() {
        super("speed");
    }

    @Override
    protected SpeedTuple transformTuple(TridentTuple tuple) {
        return new SpeedTuple(
                tuple.getIntegerByField("id"),
                Timestamp.parse(tuple.getStringByField("timestamp")),
                tuple.getDoubleByField("latitude"),
                tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("speed")
        );
    }

    public static class SpeedTuple extends TridentTupleListener.Tuple {
        public double speed;

        public SpeedTuple(int id, LocalDateTime timestamp, double latitude, double longitude, double speed) {
            super(id, timestamp, latitude, longitude);
            this.speed = speed;
        }
    }
}
