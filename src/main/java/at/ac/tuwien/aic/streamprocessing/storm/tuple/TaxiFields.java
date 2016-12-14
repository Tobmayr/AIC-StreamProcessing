package at.ac.tuwien.aic.streamprocessing.storm.tuple;

import org.apache.storm.tuple.Fields;

public class TaxiFields {


    public static Fields BASE_DISTARRAY = new Fields("id", "timestamp", "latitude", "longitude", "distanceArray");
    public static Fields BASE_FIELDS = new Fields("id", "timestamp", "latitude", "longitude");
    public static Fields BASE_SPEED_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "speed");
    public static Fields BASE_SPEED_AVG_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
    public static Fields BASE_DISTANCE_FIELDS = new Fields("id", "timestamp", "latitude", "longitude", "distance");

    public static Fields BASE_POSITIONARRAY = new Fields("id", "timestamp", "latitude", "longitude","positionArray");
    public static Fields BASE_SPEED_POSITIONARRAY = new Fields("id", "timestamp", "latitude", "longitude", "speed", "positionArray");

    public static Fields ID_POSITIONARRAY= new Fields("id", "positionArray") ;
}
