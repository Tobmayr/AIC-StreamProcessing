package at.ac.tuwien.aic.streamprocessing.trident.utils;

import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class DistanceHook extends TridentHook<DistanceHook.DistanceTuple> {

    public DistanceHook() {
        super("distance");
    }

    @Override
    protected DistanceTuple transformTuple(TridentTuple tuple) {
        return new DistanceTuple(0, null, 0.0, 0.0, 0.0);
    }

    public static class DistanceTuple extends TridentHook.Tuple {
        public double distance;

        public DistanceTuple(int id, LocalDateTime timestamp, double latitude, double longitude, double distance) {
            super(id, timestamp, latitude, longitude);
            this.distance = distance;
        }
    }
}