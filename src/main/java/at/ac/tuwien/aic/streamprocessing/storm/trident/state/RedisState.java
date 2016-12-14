package at.ac.tuwien.aic.streamprocessing.storm.trident.state;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import org.apache.storm.trident.state.State;
import org.apache.storm.trident.tuple.TridentTuple;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.stream.Collectors;

public class RedisState<T extends StateObject> implements State {

    private final String TRIDENT_STATE_REDIS_PREFIX = "tridentState";
    private final String state_key;

    private final String redisHost;
    private final int redisPort;
    private final StateObjectMapper<T> mapper;

    public RedisState(String name, String redisHost, int redisPort, StateObjectMapper<T> mapper) {
        this.state_key = TRIDENT_STATE_REDIS_PREFIX + ":" + name;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.mapper = mapper;
    }

    public StateObjectMapper<T> getMapper() {
        return mapper;
    }

    @Override
    public void beginCommit(Long txid) {
        // ignore
    }

    @Override
    public void commit(Long txid) {
        // ignore
    }

    private void set(Jedis jedis, Integer taxiId, T state) {
        String key = state_key + ":" + taxiId;
        String value = mapper.serializeToRedis(state);
        jedis.set(key, value);
    }

    private T get(Jedis jedis, Integer taxiId) {
        String value = jedis.get(state_key + ":" + taxiId);
        if (value == null) {
            return null;
        } else {
            return mapper.deserializeFromRedis(value);
        }
    }

    public List<T> getAll(List<Integer> ids) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        List<T> states = ids.stream()
                .map(id -> get(jedis, id))
                .collect(Collectors.toList());

        jedis.close();

        return states;
    }

    public void setAll(List<Integer> ids, List<T> states) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        for (int i = 0; i < ids.size(); i++) {
            Integer id = ids.get(i);
            T state = states.get(i);

            set(jedis, id, state);
        }

        jedis.close();
    }

    protected List<T> transformTuples(List<TridentTuple> tuples) {
        return tuples.stream()
                .map(getMapper()::fromTuple)
                .collect(Collectors.toList());
    }
}
