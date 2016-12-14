package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class SpeedDB implements State {
    private final String redisHost;
    private final int redisPort;
    private Jedis jedis;
    private String prefix = "tridentState";

    public SpeedDB(String type, String redisHost, int redisPort) {
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

    public void setLocationsBulk(List<Integer> taxiIds, List<Position> locationsWithDistance) {
        jedis = new Jedis(redisHost, redisPort);
        for (int index = 0; index < taxiIds.size(); index++) {
            Position location = locationsWithDistance.get(index);
            String key = this.prefix + ":" + taxiIds.get(index);
            String value = location.toRedisValue();
            jedis.set(key ,value );
        }
        jedis.close();
    }

    public List<Position> bulkGetLocations(List<Integer> userIds) {
        jedis = new Jedis(redisHost, redisPort);

        ArrayList<Position> locations = new ArrayList<>();
        for (Integer id: userIds) {
            String location = jedis.get(this.prefix + ":" + id);
            if (location == null) {
                locations.add(null);
            } else {
                locations.add(Position.fromRedisValue(location));
            }
        }

        jedis.close();
        return locations;
    }
}
