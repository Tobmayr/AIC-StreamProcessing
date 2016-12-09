package at.ac.tuwien.aic.streamprocessing.storm.trident;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import redis.clients.jedis.Jedis;

public class StoreInformation extends BaseFilter {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;

    public enum OperatorType {
        AVERGAGE_SPEED, DISTANCE
    }

    private OperatorType type;

    public StoreInformation(OperatorType type) {
        this.type = type;

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        String key = "", value = "";

        if (type.equals(OperatorType.AVERGAGE_SPEED)) {
            key = "" + tuple.getIntegerByField("id") + "_avg";
            value = "" + tuple.getDoubleByField("avgSpeed");
        } else if (type.equals(OperatorType.DISTANCE)) {
            key = "" + tuple.getIntegerByField("id") + "_dist";
            value = "" + tuple.getDoubleByField("distance");
        }
        if (!key.isEmpty() && !value.isEmpty()) {
            Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
            jedis.set(key, value);
            jedis.close();
        }
        return false;
    }
}
