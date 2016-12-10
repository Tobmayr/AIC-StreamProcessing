package at.ac.tuwien.aic.streamprocessing.trident.utils;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class AvgSpeedTupleListener extends TridentTupleListener<AvgSpeedTupleListener.AvgSpeedTuple> {

    public AvgSpeedTupleListener() {
        super("avgSpeed");
    }

    @Override
    protected AvgSpeedTuple transformTuple(TridentTuple tuple) {
        return new AvgSpeedTuple(
                tuple.getIntegerByField("id"),
                Timestamp.parse(tuple.getStringByField("timestamp")),
                tuple.getDoubleByField("latitude"),
                tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("speed"),
                tuple.getDoubleByField("avgSpeed")
        );
    }

    public static class AvgSpeedTuple extends SpeedTupleListener.SpeedTuple {
        public double avgSpeed;

        public AvgSpeedTuple(int id, LocalDateTime timestamp, double latitude, double longitude, double speed, double avgSpeed) {
            super(id, timestamp, latitude, longitude, speed);
            this.avgSpeed = avgSpeed;
        }
    }
}