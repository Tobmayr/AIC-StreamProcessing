package at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed;

import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;

public class SpeedStateMapper implements StateObjectMapper<SpeedState> {

    @Override
    public Values toStateTuple(SpeedState state) {
        return new Values(state.getTimestamp(), state.getLatitude(), state.getLongitude(), state.getSpeed(), true);
    }

    @Override
    public Values createInitialStateTuple() {
        String timestamp = "";
        Double latitude = 0.0;
        Double longitude = 0.0;
        Double speed = 0.0;
        boolean actual_state = false;

        return new Values(timestamp, latitude, longitude, speed, actual_state);
    }

    @Override
    public SpeedState fromTuple(TridentTuple tuple) {
        return new SpeedState(tuple.getStringByField("timestamp"), tuple.getDoubleByField("latitude"), tuple.getDoubleByField("longitude"),
                tuple.getDoubleByField("speed"));
    }

    @Override
    public Values toTuple(Integer id, SpeedState state) {
        return new Values(id, state.getTimestamp(), state.getLatitude(), state.getLongitude(), state.getSpeed());
    }

    @Override
    public SpeedState parseState(TridentTuple tuple) {
        return new SpeedState(tuple.getStringByField("prev_timestamp"), tuple.getDoubleByField("prev_latitude"), tuple.getDoubleByField("prev_longitude"),
                tuple.getDoubleByField("speed"));

    }

    @Override
    public String serializeToRedis(SpeedState state) {
        return String.format("%s,%s,%s,%s", state.getTimestamp(), state.getLatitude(), state.getLongitude(), state.getSpeed());
    }

    @Override
    public SpeedState deserializeFromRedis(String value) {
        String parts[] = value.split(",");

        String timestamp = parts[0];
        Double latitude = Double.parseDouble(parts[1]);
        Double longitude = Double.parseDouble(parts[2]);
        Double speed = Double.parseDouble(parts[3]);

        return new SpeedState(timestamp, latitude, longitude, speed);
    }
}
