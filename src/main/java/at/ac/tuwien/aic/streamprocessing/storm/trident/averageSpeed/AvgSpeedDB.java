package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisStateObjectMapper;
import org.apache.storm.trident.state.State;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

public class AvgSpeedDB extends RedisState<AvgSpeed, AvgSpeedDB.AvgSpeedMapper> {
    public AvgSpeedDB(String redisHost, int redisPort) {
        super("avg_speed", redisHost, redisPort, new AvgSpeedMapper());
    }

    public static class AvgSpeedMapper implements RedisStateObjectMapper<AvgSpeed> {
        @Override
        public String serializeToRedis(AvgSpeed stateObject) {
            return stateObject.lastTimestamp + "," + stateObject.avgSpeed + "," + stateObject.hours;
        }

        @Override
        public AvgSpeed deserializeFromRedis(String value) {
            String parts[] = value.split(",");
            AvgSpeed speed = new AvgSpeed();
            speed.lastTimestamp = parts[0];
            speed.avgSpeed = Double.parseDouble(parts[1]);
            speed.hours = Double.parseDouble(parts[2]);
            return speed;
        }
    }
}
