package at.ac.tuwien.aic.streamprocessing.storm.trident.distance;

import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;
import java.util.ArrayList;

import java.util.List;

public class DistanceDB implements State {
    private final String redisHost;
    private final int redisPort;
    private Jedis jedis;
    private String prefix = "tridentState";

    public DistanceDB(String type, String redisHost, int redisPort) {
        this.prefix = this.prefix + ":" + type;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    public void beginCommit(Long txid) {
        //jedis.multi(); // TODO start transaction
    }

    public void commit(Long txid) {
        //jedis.exec(); // TODO commit transaction
    }

    public void setLocationsBulk(List<Integer> taxiIds, List<ArrayList<Double>> locationsWithDistance) {
        System.out.println("DistanceDB"+taxiIds+locationsWithDistance);
        jedis = new Jedis(redisHost, redisPort);
        for (int index = 0; index < taxiIds.size(); index++) {
            ArrayList<Double> location = locationsWithDistance.get(index);
            String key = this.prefix + ":" + taxiIds.get(index);
            String value = location.get(0) + "," + location.get(1) + "," + location.get(2);
            jedis.set(key ,value );
        }
        jedis.close();
    }

    public List<ArrayList<Double>> bulkGetLocations(List<Integer> userIds) {
        jedis = new Jedis(redisHost, redisPort);

        ArrayList<ArrayList<Double>> locations = new ArrayList<>();
        for (Integer id: userIds) {
            String location = jedis.get(this.prefix + ":" + id);
            if (location == null) {
                locations.add(null);
            } else {
                String locationStrings[] = location.split(",");
                ArrayList<Double> locationDouble = new ArrayList<>();
                locationDouble.add(Double.parseDouble(locationStrings[0]));
                locationDouble.add(Double.parseDouble(locationStrings[1]));
                locationDouble.add(Double.parseDouble(locationStrings[2]));
                locations.add(locationDouble);
            }
        }
        System.out.println("DistanceDB fetched"+userIds+locations);

        jedis.close();
        return locations;
    }
}
