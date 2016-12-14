package at.ac.tuwien.aic.streamprocessing.storm.tuple;

import org.apache.storm.tuple.Fields;

public class TaxiFields {
    public static Fields BASE_FIELDS = new Fields("id", "timestamp", "latitude", "longitude");

    // speed tuple fields
    public static Fields SPEED_STATE_FIELDS = new Fields("prev_timestamp", "prev_latitude", "prev_longitude","speed", "has_state");
    public static Fields CALCULATE_SPEED_INPUT_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "prev_timestamp", "prev_latitude", "prev_longitude", "speed", "has_state");
    public static Fields CALCULATE_SPEED_OUTPUT_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "speed");

    // avg speed tuple fields
    public static Fields ID_AND_SPEED_FIELDS = new Fields("id", "speed");
    public static Fields AVG_SPEED_STATE_FIELDS = new Fields("observations", "speedSum", "has_state");
    public static Fields AVG_SPEED_INPUT_FIELDS = new Fields("id", "speed", "observations", "speedSum", "has_state");
    public static Fields AVG_SPEED_OUTPUT_FIELDS = new Fields("id", "avgSpeed", "observations", "speedSum");

    // distance tuple fields
    public static Fields DISTANCE_STATE_FIELDS = new Fields("prev_latitude", "prev_longitude", "distance", "has_state");
    public static Fields CALCULATE_DISTANCE_INPUT_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "prev_latitude", "prev_longitude", "distance", "has_state");
    public static Fields CALCULATE_DISTANCE_OUTPUT_FIELDS = new Fields("id", "latitude", "longitude", "distance");
}
