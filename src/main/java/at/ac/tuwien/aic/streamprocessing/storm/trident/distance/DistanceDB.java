package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisStateObjectMapper;

import java.util.ArrayList;

public class DistanceDB extends RedisState<ArrayList<Double>, DistanceDB.DistanceMapper> {
    public DistanceDB(String redisHost, int redisPort) {
        super("distance", redisHost, redisPort, new DistanceMapper());
    }

    public static class DistanceMapper implements RedisStateObjectMapper<ArrayList<Double>> {
        @Override
        public String serializeToRedis(ArrayList<Double> stateObject) {
            return stateObject.get(0) + "," + stateObject.get(1) + "," + stateObject.get(2);
        }

        @Override
        public ArrayList<Double> deserializeFromRedis(String value) {
            String locationStrings[] = value.split(",");
            ArrayList<Double> locationDouble = new ArrayList<>();
            locationDouble.add(Double.parseDouble(locationStrings[0]));
            locationDouble.add(Double.parseDouble(locationStrings[1]));
            locationDouble.add(Double.parseDouble(locationStrings[2]));
            return locationDouble;
        }
    }
}
