package at.ac.tuwien.aic.streamprocessing.trident.utils;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;

public class DistanceTupleListener extends TridentTupleListener<DistanceTupleListener.DistanceTuple> {

    public DistanceTupleListener() {
        super("distance");
    }

    @Override
    protected DistanceTuple transformTuple(TridentTuple tuple) {
        return new DistanceTuple(
                tuple.getIntegerByField("id"),
                tuple.getDoubleByField("latitude"),
                tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("distance")
        );
    }

    public static class DistanceTuple extends TridentTupleListener.Tuple {
        public double distance;

        public DistanceTuple(Integer id, double latitude, double longitude, double distance) {
            super(id, null, latitude, longitude);
            this.distance = distance;
        }
    }
}