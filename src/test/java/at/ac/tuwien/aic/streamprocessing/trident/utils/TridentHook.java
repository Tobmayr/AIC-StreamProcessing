package at.ac.tuwien.aic.streamprocessing.trident.utils;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TridentHook<T> extends BaseFilter {

    private String name;
    private static Map<String, List<Object>> tuplesByName = new ConcurrentHashMap<>();

    public TridentHook(String name) {
        this.name = name;

        tuplesByName.put(name, Collections.synchronizedList(new ArrayList<>()));
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        tuplesByName.get(name).add(transformTuple(tuple));

        System.out.println("[" + name + "]: " + tuple);

        return true;
    }

    public List<T> getTuples() {
        return (List<T>)tuplesByName.get(name);
    }

    protected abstract T transformTuple(TridentTuple tuple);

    public static class Tuple {

        public int id;
        public LocalDateTime timestamp;
        public double latitude;
        public double longitude;

        public Tuple(int id, LocalDateTime timestamp, double latitude, double longitude) {
            this.id = id;
            this.timestamp = timestamp;
            this.latitude = latitude;
            this.longitude = longitude;
        }

        public static LocalDateTime parseDateTime(String s) {
            return Timestamp.parse(s);
        }
    }
}
