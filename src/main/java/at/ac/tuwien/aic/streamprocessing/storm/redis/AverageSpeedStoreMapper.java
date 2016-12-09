package at.ac.tuwien.aic.streamprocessing.storm.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class AverageSpeedStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "taxiID_avgSpeed";
    private static final String ID_POSTFIX="_avgSpeed";
    

    public AverageSpeedStoreMapper() {
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return "" + tuple.getIntegerByField("id") + ID_POSTFIX;
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return "" + tuple.getDoubleByField("avgSpeed");
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

}
