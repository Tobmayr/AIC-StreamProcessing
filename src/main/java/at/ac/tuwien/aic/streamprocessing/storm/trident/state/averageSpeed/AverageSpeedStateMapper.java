package at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class AverageSpeedStateMapper implements StateObjectMapper<AverageSpeedState> {

    @Override
    public Values toStateTuple(AverageSpeedState state) {
        return new Values(state.getObservations(), state.getSpeedSum(), true);
    }

    @Override
    public Values createInitialStateTuple() {
        int observations = 0;
        double speedSum = 0.0;
        boolean actual_state = false;

        return new Values(observations, speedSum, actual_state);
    }

    @Override
    public AverageSpeedState fromTuple(TridentTuple tuple) {
        return new AverageSpeedState(
                tuple.getIntegerByField("observations"),
                tuple.getDoubleByField("speedSum")
        );
    }

    @Override
    public Values toTuple(Integer id, AverageSpeedState state) {
        Double avgSpeed = state.getSpeedSum() / state.getObservations();
        return new Values(id, avgSpeed, state.getObservations(), state.getSpeedSum());
    }

    @Override
    public AverageSpeedState parseState(TridentTuple tuple) {
        return new AverageSpeedState(
                tuple.getIntegerByField("observations"),
                tuple.getDoubleByField("speedSum")
        );
    }

    @Override
    public String serializeToRedis(AverageSpeedState state) {
        return String.format("%s,%s",
                state.getObservations(), state.getSpeedSum()
        );
    }

    @Override
    public AverageSpeedState deserializeFromRedis(String value) {
        String parts[] = value.split(",");

        Integer observations = Integer.parseInt(parts[0]);
        Double speedSum = Double.parseDouble(parts[1]);
        return new AverageSpeedState(observations, speedSum);
    }
}
