package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class PropagateLocation extends DashboardNotifier {
    private Map<Integer, Long> idTimeMap = Collections.synchronizedMap(new HashMap<Integer, Long>());
    private final Logger logger = LoggerFactory.getLogger(PropagateLocation.class);

    public PropagateLocation(String dashboardAddress) {
        super(dashboardAddress + Constants.PROPAGATE_LOCATION_URI);

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        Long currentTime = System.currentTimeMillis();
        Long previousTime = idTimeMap.get(taxiId);
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");

        if ((previousTime == null) || (currentTime - previousTime >= Constants.PROPAGATE_LOCATION_PERIOD)) {
            propagateToDashboard(taxiId, latitude, longitude);
            idTimeMap.put(taxiId, currentTime);
        }

        return true;
    }

    private void propagateToDashboard(Integer taxiId, Double latitude, Double longitude) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiId", Integer.toString(taxiId));
        map.put("latitude", Double.toString(latitude));
        map.put("longitude", Double.toString(longitude));
        sendJSONPostRequest(map);

    }
}
