package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;
import java.util.ArrayList;

import java.util.List;

public class DistanceDB extends RedisState<ArrayList<Double>> {
    public DistanceDB(String redisHost, int redisPort) {
        super("distance", redisHost, redisPort);
    }

    @Override
    protected ArrayList<Double> parse(String value) {
        String locationStrings[] = value.split(",");
        ArrayList<Double> locationDouble = new ArrayList<>();
        locationDouble.add(Double.parseDouble(locationStrings[0]));
        locationDouble.add(Double.parseDouble(locationStrings[1]));
        locationDouble.add(Double.parseDouble(locationStrings[2]));
        return locationDouble;
    }

    @Override
    protected String serialize(ArrayList<Double> state) {
        return state.get(0) + "," + state.get(1) + "," + state.get(2);
    }
}
