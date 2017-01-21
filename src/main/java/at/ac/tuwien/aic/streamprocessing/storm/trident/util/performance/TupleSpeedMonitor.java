package at.ac.tuwien.aic.streamprocessing.storm.trident.util.performance;

import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class TupleSpeedMonitor implements Filter {
    private static final long UPDATE_INTERVAL = 100;
    private static final Logger logger = LoggerFactory.getLogger(TupleSpeedMonitor.class);


    private String prefix;
    private String redisHost;
    private int redisPort;

    private Long firstTupleEncountered = null;
    private AtomicLong tuples = new AtomicLong();

    public TupleSpeedMonitor(String prefix, String redisHost, int redisPort) {
        this.prefix = prefix;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        if (firstTupleEncountered == null) {
            firstTupleEncountered = System.currentTimeMillis();
        }

        long current = tuples.incrementAndGet();

        if (current % UPDATE_INTERVAL == 0) {
            store(current);
        }

        return true;
    }

    private void store(Long counter) {
        long now = System.currentTimeMillis();

        Double tuplesPerSecond = counter / ((now - firstTupleEncountered) / 1000.0);
        String s = String.format("%.2f", tuplesPerSecond);

        Jedis jedis = new Jedis(redisHost, redisPort);
        jedis.set(prefix + "_total_tuples", counter.toString());
        jedis.set(prefix + "_tuples_per_second", s);
        jedis.close();

        logger.debug(prefix + ":" + counter + " tuples (" + s + " tuples/s)");
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
