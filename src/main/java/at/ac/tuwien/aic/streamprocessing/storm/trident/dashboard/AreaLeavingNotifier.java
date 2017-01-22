package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;

public class AreaLeavingNotifier extends DashboardNotifier {

    public AreaLeavingNotifier(String dashboardAddress) {
        super(dashboardAddress + Constants.NOTIFY_AREA_VIOLATION_URI);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        Double centerLat = Constants.ALLOWED_AREA_CENTER_LAT;
        Double centerLong = Constants.ALLOWED_AREA_CENTER_LONG;
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");
        Double distance = Haversine.calculateDistanceBetween(centerLat, centerLong, latitude, longitude);
        if (distance >= Constants.WARNING_DISTANCE) {
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            map.put("distance", Double.toString(distance));
            sendJSONPostRequest(map);
        }
        return true;
    }
}