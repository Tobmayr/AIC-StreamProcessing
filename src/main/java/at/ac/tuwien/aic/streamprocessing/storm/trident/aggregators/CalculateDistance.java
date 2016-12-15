package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CalculateDistance extends Aggregator<DistanceState> {
    private final Logger logger = LoggerFactory.getLogger(CalculateDistance.class);

    private DistanceStateMapper mapper;

    public CalculateDistance() {
        super(true);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        this.mapper = new DistanceStateMapper();
    }

    @Override
    protected DistanceState compute(DistanceState previous, TridentTuple tuple) {
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");

        Double delta = Haversine.calculateDistanceBetween(previous.getLatitude(), previous.getLongitude(), latitude, longitude);
        Double distance = previous.getDistance() + delta;

        logger.debug(
                "(distance): [taxiId={}, timestamp={}, latitude={}, longitude={}, distance={}]",
                tuple.getIntegerByField("id"),
                tuple.getStringByField("timestamp"),
                latitude,
                longitude,
                String.format("%.3f", distance)
        );


        return new DistanceState(latitude, longitude, distance);
    }

    @Override
    protected StateObjectMapper<DistanceState> getMapper() {
        return mapper;
    }
}
