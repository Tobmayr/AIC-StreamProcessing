package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.optimization;

import java.util.HashMap;
import java.util.Map;

import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.DashboardNotifier;
import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;

public class OptimizedAreaLeavingNotifier extends DashboardNotifier {

    public OptimizedAreaLeavingNotifier(String dashboardAddress) {
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

        // the taxi is more than 15 km away from the forbidden city, the dashboard should discard this taxi
        if (distance >= Constants.PROHIBITED_DISTANCE) {
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            map.put("discard", "true");
            sendJSONPostRequest(map);
            return false;
        }

        // the taxi is more than 10 km away from the forbidden city, warn the dashboard
        if (distance >= Constants.WARNING_DISTANCE) {
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            map.put("distance", Double.toString(distance));
            sendJSONPostRequest(map);
        }

        return true;

    }

}
