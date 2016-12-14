package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisStateObjectMapper;
import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class SpeedDB extends RedisState<Position, SpeedDB.SpeedMapper> {
    public SpeedDB(String redisHost, int redisPort) {
        super("position", redisHost, redisPort, new SpeedMapper());
    }

    public static class SpeedMapper implements RedisStateObjectMapper<Position> {
        @Override
        public String serializeToRedis(Position stateObject) {
            return stateObject.timestamp + ',' + stateObject.latitude + "," + stateObject.longitude;
        }

        @Override
        public Position deserializeFromRedis(String value) {
            String parts[] = value.split(",");
            Position position = new Position();
            position.timestamp = parts[0];
            position.latitude = Double.parseDouble(parts[1]);
            position.longitude = Double.parseDouble(parts[2]);
            return position;
        }
    }
}
