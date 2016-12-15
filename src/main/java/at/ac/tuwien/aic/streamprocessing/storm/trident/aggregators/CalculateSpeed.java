package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

public class CalculateSpeed extends Aggregator<SpeedState> {

    public CalculateSpeed() {
        super(true);
    }

    private StateObjectMapper<SpeedState> mapper;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        this.mapper = new SpeedStateMapper();
    }

    @Override
    protected SpeedState compute(SpeedState previous, TridentTuple tuple) {
        String timestamp = tuple.getStringByField("timestamp");
        Double currentLatitude = tuple.getDoubleByField("latitude");
        Double currentLongitude = tuple.getDoubleByField("longitude");

        Double distance = Haversine.calculateDistanceBetween(
                previous.getLatitude(), previous.getLongitude(),
                currentLatitude, currentLongitude);

        LocalDateTime startTime = Timestamp.parse(previous.getTimestamp());
        LocalDateTime endTime = Timestamp.parse(timestamp);

        Double time = ChronoUnit.MILLIS.between(startTime, endTime) / (60. * 60.0 * 1000.0);

        Double speed;
        if (Double.compare(time, 0.0) == 0) {
            speed = 0.0;
        } else {
            speed = distance / time;  // in kmh
        }

        return new SpeedState(timestamp, currentLatitude, currentLongitude, speed);
    }

    @Override
    protected StateObjectMapper<SpeedState> getMapper() {
        return mapper;
    }
}
