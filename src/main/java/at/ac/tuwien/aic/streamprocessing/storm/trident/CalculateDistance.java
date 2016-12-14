package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CalculateDistance extends Aggregator<DistanceState> {

    private DistanceStateMapper mapper;

    public CalculateDistance() {
        super(true);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        this.mapper = new DistanceStateMapper();
    }

    private final Logger logger = LoggerFactory.getLogger(CalculateDistance.class);

    @Override
    protected DistanceState compute(DistanceState previous, TridentTuple tuple) {
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");

        Double delta = Haversine.haversine(previous.getLatitude(), previous.getLongitude(), latitude, longitude);

        return new DistanceState(latitude, longitude, previous.getDistance() + delta);
    }

    @Override
    protected StateObjectMapper<DistanceState> getMapper() {
        return mapper;
    }
}
