package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CalculateAverageSpeed extends Aggregator<AverageSpeedState> {
    private final Logger logger = LoggerFactory.getLogger(CalculateAverageSpeed.class);

    private StateObjectMapper<AverageSpeedState> mapper;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);

        this.mapper = new AverageSpeedStateMapper();
    }

    @Override
    protected AverageSpeedState compute(AverageSpeedState previous, TridentTuple tuple) {
        Double speedSum = previous.getSpeedSum();
        Integer observations = previous.getObservations();
        Double speed = tuple.getDoubleByField("speed");

        Double newSpeedSum = speedSum + speed;
        Integer newObservations = observations + 1;

        Double averageSpeed = newSpeedSum / newObservations;

        logger.debug(
                "(avgSpeed): [taxiId={}, timestamp={}, avgSpeed={}]",
                tuple.getIntegerByField("id"),
                tuple.getStringByField("timestamp"),
                String.format("%.3f", averageSpeed)
        );


        return new AverageSpeedState(newObservations, newSpeedSum);
    }

    @Override
    protected StateObjectMapper<AverageSpeedState> getMapper() {
        return mapper;
    }
}
