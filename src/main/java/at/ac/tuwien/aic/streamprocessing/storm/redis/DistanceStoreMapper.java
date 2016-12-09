package at.ac.tuwien.aic.streamprocessing.storm.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

public class DistanceStoreMapper implements RedisStoreMapper {
    private RedisDataTypeDescription description;
    private final String hashKey = "taxiID_dist";
    private static final String ID_POSTFIX = "_dist";

    public DistanceStoreMapper() {
        description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    @Override
    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getIntegerByField("id") + ID_POSTFIX;
    }

    @Override
    public String getValueFromTuple(ITuple tuple) {
        return "" + tuple.getDoubleByField("distance");
    }

    @Override
    public RedisDataTypeDescription getDataTypeDescription() {
        // TODO Auto-generated method stub
        return null;
    }

}
