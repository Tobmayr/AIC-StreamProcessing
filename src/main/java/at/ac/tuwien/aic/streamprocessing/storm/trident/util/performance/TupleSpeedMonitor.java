package at.ac.tuwien.aic.streamprocessing.storm.trident.util.performance;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

public class TupleSpeedMonitor implements Filter {
	private static final long UPDATE_INTERVAL = 100;
	private static final Logger logger = LoggerFactory.getLogger(TupleSpeedMonitor.class);

	private static Map<String, MonitorData> dataMap = new HashMap<>();
	private String prefix;
	private String redisHost;
	private int redisPort;
	private int partitionIndex;

	public TupleSpeedMonitor(String prefix, String redisHost, int redisPort) {
		dataMap.put(prefix, new MonitorData());
		this.prefix = prefix;
		this.redisHost = redisHost;
		this.redisPort = redisPort;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		MonitorData data = dataMap.get(prefix);
		if (data.firstTupleEncountered == null) {
			data.firstTupleEncountered = System.currentTimeMillis();
		}

		long current = data.tuples.incrementAndGet();

		if (current % UPDATE_INTERVAL == 0) {
			store(current, data);
		}

		return true;
	}

	private void store(Long counter, MonitorData data) {
		long now = System.currentTimeMillis();

		Double tuplesPerSecond = counter / ((now - data.firstTupleEncountered) / 1000.0);
		String s = String.format("%.2f", tuplesPerSecond);

		Jedis jedis = new Jedis(redisHost, redisPort);
		jedis.set(prefix + "_total_tuples", counter.toString());
		jedis.set(prefix + "_tuples_per_second", s);
		jedis.close();

		logger.info(String.format("[P:%s]%s:%s tuples (%s tuples/s)", partitionIndex, prefix, counter, s));

	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {

	}

	private class MonitorData {

		public Long firstTupleEncountered = null;
		public AtomicLong tuples = new AtomicLong();

	}
}
