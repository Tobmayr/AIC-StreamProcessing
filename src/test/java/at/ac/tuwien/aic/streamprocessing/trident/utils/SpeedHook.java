package at.ac.tuwien.aic.streamprocessing.trident.utils;

import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class SpeedHook extends TridentHook<SpeedHook.SpeedTuple> {

    public SpeedHook() {
        super("speed");
    }

    @Override
    protected SpeedTuple transformTuple(TridentTuple tuple) {
        return new SpeedTuple(0, null, 0.0, 0.0, 0.0);
    }

    public static class SpeedTuple extends TridentHook.Tuple {
        public double speed;

        public SpeedTuple(int id, LocalDateTime timestamp, double latitude, double longitude, double speed) {
            super(id, timestamp, latitude, longitude);
            this.speed = speed;
        }
    }
}
