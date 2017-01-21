package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.optimization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.DashboardNotifier;
import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;

public class OptimizedAreaLeavingNotifierAndLocationPropagator extends DashboardNotifier {

    private Map<Integer, Long> idTimeMap = Collections.synchronizedMap(new HashMap<Integer, Long>());
    private String dashboardAddress;

    public OptimizedAreaLeavingNotifierAndLocationPropagator(String dashboardAddress) {
        super(dashboardAddress + Constants.NOTIFY_AREA_VIOLATION_URI);
        this.dashboardAddress = dashboardAddress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        Long currentTime = System.currentTimeMillis();
        Long previousTime = idTimeMap.get(taxiId);
        Double centerLat = Constants.ALLOWED_AREA_CENTER_LAT;
        Double centerLong = Constants.ALLOWED_AREA_CENTER_LONG;
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");
        Double distance = Haversine.calculateDistanceBetween(centerLat, centerLong, latitude, longitude);

        Map<String, String> map = new HashMap<>();
        map.put("taxiId", Integer.toString(taxiId));

        // update the dashboard every 5 seconds
        if ((previousTime == null) || (currentTime - previousTime >= Constants.PROPAGATE_LOCATION_PERIOD)) {
            map.put("latitude", Double.toString(latitude));
            map.put("longitude", Double.toString(longitude));
            sendJSONPostRequest(map, dashboardAddress + Constants.PROPAGATE_LOCATION_URI);
            idTimeMap.put(taxiId, currentTime);
        }

        // the taxi is more than 10 km away from the forbidden city, warn the dashboard
        if (distance >= Constants.WARNING_DISTANCE) {
            map.put("distance", Double.toString(distance));
            sendJSONPostRequest(map);
        }

        // the taxi is more than 15 km away from the forbidden city, discard this tuple
        if (distance >= Constants.PROHIBITED_DISTANCE) {
            return false;
        }

        return true;

    }

}
