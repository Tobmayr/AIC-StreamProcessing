package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class AvgSpeedDB extends RedisState<AvgSpeed> {
    public AvgSpeedDB(String redisHost, int redisPort) {
        super("avg_speed", redisHost, redisPort);
    }

    @Override
    protected AvgSpeed parse(String value) {
        return AvgSpeed.fromRedisValue(value);
    }

    @Override
    protected String serialize(AvgSpeed state) {
        return state.toRedisValue();
    }
}
