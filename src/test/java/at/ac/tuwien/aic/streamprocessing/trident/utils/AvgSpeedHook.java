package at.ac.tuwien.aic.streamprocessing.trident.utils;

import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class AvgSpeedHook extends TridentHook<AvgSpeedHook.AvgSpeedTuple> {

    public AvgSpeedHook() {
        super("avgSpeed");
    }

    @Override
    protected AvgSpeedTuple transformTuple(TridentTuple tuple) {
        return new AvgSpeedTuple(
                tuple.getIntegerByField("id"),
                Tuple.parseDateTime(tuple.getStringByField("timestamp")),
                tuple.getDoubleByField("latitude"),
                tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("speed"),
                tuple.getDoubleByField("avgSpeed")
        );
    }

    public static class AvgSpeedTuple extends SpeedHook.SpeedTuple {
        public double avgSpeed;

        public AvgSpeedTuple(int id, LocalDateTime timestamp, double latitude, double longitude, double speed, double avgSpeed) {
            super(id, timestamp, latitude, longitude, speed);
            this.avgSpeed = avgSpeed;
        }
    }
}