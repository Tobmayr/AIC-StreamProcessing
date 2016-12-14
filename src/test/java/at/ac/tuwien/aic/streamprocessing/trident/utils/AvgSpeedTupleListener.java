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
                tuple.getDoubleByField("avgSpeed")
        );
    }

    public static class AvgSpeedTuple extends SpeedTupleListener.SpeedTuple {
        public double avgSpeed;

        public AvgSpeedTuple(int id, double avgSpeed) {
            super(id, null, 0.0, 0.0, 0.0);
            this.avgSpeed = avgSpeed;
        }
    }
}