package at.ac.tuwien.aic.streamprocessing.storm.tuple;

import org.apache.storm.tuple.Fields;

public class TaxiFields {
    public static Fields BASE_FIELDS = new Fields("id", "timestamp", "latitude", "longitude");
    public static Fields BASE_SPEED_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "speed");
    public static Fields BASE_SPEED_AVG_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
    public static Fields BASE_DISTANCE_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "distance");
}