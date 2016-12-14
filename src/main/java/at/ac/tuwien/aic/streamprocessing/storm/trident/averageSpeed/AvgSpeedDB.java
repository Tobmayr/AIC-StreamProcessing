package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class AvgSpeedDB implements State {
    private final String redisHost;
    private final int redisPort;
    private Jedis jedis;
    private String prefix = "tridentState";

    public AvgSpeedDB(String type, String redisHost, int redisPort) {
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

    public void setLocationsBulk(List<Integer> taxiIds, List<AvgSpeed> locationsWithDistance) {
        jedis = new Jedis(redisHost, redisPort);
        for (int index = 0; index < taxiIds.size(); index++) {
            AvgSpeed location = locationsWithDistance.get(index);
            String key = this.prefix + ":" + taxiIds.get(index);
            String value = location.toRedisValue();
            jedis.set(key ,value );
        }
        jedis.close();
    }

    public List<AvgSpeed> bulkGetLocations(List<Integer> userIds) {
        jedis = new Jedis(redisHost, redisPort);

        ArrayList<AvgSpeed> locations = new ArrayList<>();
        for (Integer id: userIds) {
            String location = jedis.get(this.prefix + ":" + id);
            if (location == null) {
                locations.add(null);
            } else {
                locations.add(AvgSpeed.fromRedisValue(location));
            }
        }

        jedis.close();
        return locations;
    }
}
