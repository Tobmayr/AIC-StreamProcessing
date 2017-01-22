package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.optimization;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.DashboardNotifier;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;

public class OptimizedAreaLeavingNotifierAndLocationPropagator extends DashboardNotifier {

	private Map<Integer, Long> idTimeMap = Collections.synchronizedMap(new HashMap<Integer, Long>());
	private String dashboardAddress;

	public OptimizedAreaLeavingNotifierAndLocationPropagator(String dashboardAddress) {
		super(dashboardAddress + Constants.OPTIMIZED_PROPAGATE_LOCATION_URI);
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

		// update the dashboard every 5 seconds
		if ((previousTime == null) || (currentTime - previousTime >= Constants.PROPAGATE_LOCATION_PERIOD)) {
			Map<String, String> map = new HashMap<>();
			map.put("taxiId", Integer.toString(taxiId));
			map.put("latitude", Double.toString(latitude));
			map.put("longitude", Double.toString(longitude));
			map.put("distance", Double.toString(distance));
			
			String violation = "none";
			if (distance >= Constants.PROHIBITED_DISTANCE) {
				violation = "WARNING";
			} else if (distance >= Constants.WARNING_DISTANCE) {
				violation = "REMOVE";
			}
			map.put("violation", violation);
			sendJSONPostRequest(map);
			idTimeMap.put(taxiId, currentTime);
		}

		// the taxi is more than 15 km away from the forbidden city, discard
		// this tuple
		if (distance >= Constants.PROHIBITED_DISTANCE) {
			return false;
		}

		return true;

	}

}
