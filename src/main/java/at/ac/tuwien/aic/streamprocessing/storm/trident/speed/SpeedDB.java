package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class SpeedDB extends RedisState<Position> {
    public SpeedDB(String redisHost, int redisPort) {
        super("position", redisHost, redisPort);
    }

    @Override
    protected Position parse(String value) {
        return Position.fromRedisValue(value);
    }

    @Override
    protected String serialize(Position state) {
        return state.toRedisValue();
    }
}
