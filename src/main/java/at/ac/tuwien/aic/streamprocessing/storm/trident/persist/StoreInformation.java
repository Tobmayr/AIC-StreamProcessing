package at.ac.tuwien.aic.streamprocessing.storm.trident.persist;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

public class StoreInformation extends BaseFilter {
    private final String redisHost;
    private final int redisPort;
    private final Logger logger = LoggerFactory.getLogger(StoreInformation.class);

    private InfoType infoType;

    public StoreInformation(InfoType infoType, String redisHost, int redisPort) {
        this.infoType = infoType;
        this.redisHost = redisHost;
        this.redisPort = redisPort;

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {

        String key = "", value = "";
        key = "" + tuple.getIntegerByField("id") + infoType.getKeyPrefix();
        value = "" + tuple.getDoubleByField(infoType.getFieldName());
        if (!key.isEmpty() && !value.isEmpty()) {
            Jedis jedis = new Jedis(redisHost, redisPort);
            jedis.set(key, value);
            logger.debug("Set key {} with value {}", key, value);
            jedis.close();
        }
        return true;
    }
}
