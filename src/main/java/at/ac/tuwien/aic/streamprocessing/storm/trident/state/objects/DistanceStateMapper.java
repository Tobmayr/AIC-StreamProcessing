package at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class DistanceStateMapper implements StateObjectMapper<DistanceState> {

    @Override
    public Values toStateTuple(at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState state) {
        boolean actual_state = true;

        return new Values(
                state.getLatitude(),
                state.getLongitude(),
                state.getDistance(),
                actual_state
        );
    }

    @Override
    public Values createInitialStateTuple() {
        double latitude = 0.0;
        double longitude = 0.0;
        double distance = 0.0;
        boolean actual_state = false;

        return new Values(latitude, longitude, distance, actual_state);
    }

    @Override
    public DistanceState fromTuple(TridentTuple tuple) {
        return new at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState(
                tuple.getDoubleByField("latitude"),
                tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("distance")
        );
    }

    @Override
    public Values toTuple(Integer id, DistanceState state) {
        return new Values(id, state.getLatitude(), state.getLongitude(), state.getDistance());
    }

    @Override
    public DistanceState parseState(TridentTuple tuple) {
        return new at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.DistanceState(
                tuple.getDoubleByField("prev_latitude"),
                tuple.getDoubleByField("prev_longitude"),
                tuple.getDoubleByField("distance")
        );
    }

    @Override
    public String serializeToRedis(DistanceState state) {
        return String.format("%s,%s,%s",
                state.getLatitude(),
                state.getLongitude(),
                state.getDistance()
        );
    }

    @Override
    public DistanceState deserializeFromRedis(String value) {
        String parts[] = value.split(",");

        Double latitude = Double.parseDouble(parts[0]);
        Double longitude = Double.parseDouble(parts[1]);
        Double distance = Double.parseDouble(parts[2]);
        return new DistanceState(latitude, longitude, distance);
    }
}
