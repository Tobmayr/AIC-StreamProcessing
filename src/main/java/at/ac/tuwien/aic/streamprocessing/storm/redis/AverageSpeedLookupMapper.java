package at.ac.tuwien.aic.streamprocessing.storm.redis;

import java.util.List;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisLookupMapper;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Values;

public class AverageSpeedLookupMapper implements RedisLookupMapper {
	private RedisDataTypeDescription description;
	private final String hashKey = "taxiID_avgSpeed";

	public AverageSpeedLookupMapper() {
		description = new RedisDataTypeDescription(RedisDataTypeDescription.RedisDataType.HASH, hashKey);
	}

	@Override
	public String getKeyFromTuple(ITuple tuple) {
		return tuple.getStringByField("id") + "_avgSpeed";

	}

	@Override
	public String getValueFromTuple(ITuple tuple) {
		return tuple.getStringByField("avgSpeed");
	}

	@Override
	public RedisDataTypeDescription getDataTypeDescription() {
		return description;
	}

	@Override
	public List<Values> toTuple(ITuple input, Object value) {
		String id = getKeyFromTuple(input).replace("_avgSpeed", "");
		List<Values> values = Lists.newArrayList();
		values.add(new Values(id, value));
		return values;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "avgSpeed"));

	}

}
