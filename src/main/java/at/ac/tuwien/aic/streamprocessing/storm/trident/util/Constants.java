package at.ac.tuwien.aic.streamprocessing.storm.trident.util;

public class Constants {
    private Constants() {

    }

    // Allowed Area configuration
    public static final Double ALLOWED_AREA_CENTER_LAT = 39.916320;
    public static final Double ALLOWED_AREA_CENTER_LONG = 116.397155;
    public static final Double WARNING_DISTANCE = 10D;
    public static final Double BORDER_DISTANCE = 15D;

    // Speeding configuration
    public static final Double SPEED_LIMIT = 50D;
    
    //Long/Lat value which signals stop
    public static final Double STOP_TOKEN_VALUE=360D;
    
    //Time in ms for periodically sending taxi location information to dashboard
    public static final Long  PROPAGATE_LOCATION_PERIOD=5000L;

    // Dashboard sub-URI configuration
    public static final String PROPAGATE_LOCATION_URI = "/add";
    public static final String PROPAGATE_INFORMATION_URI = "/stats";
    public static final String NOTIFY_AREA_VIOLATION_URI = "/violation";
    public static final String NOTIFY_SPEEDING_INCIDENT_URI = "/incident";
    public static final String NOTIFY_TAXI_STOPPED_URI = "/stop";

}
