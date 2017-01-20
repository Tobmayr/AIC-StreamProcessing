package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.information.InformationState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class PropagateInformation extends DashboardNotifier {

	private Map<Integer, Double> idDistanceMap = Collections.synchronizedMap(new HashMap<Integer, Double>());

	public PropagateInformation(String dashboardAddress) {
		super(dashboardAddress + Constants.PROPAGATE_INFORMATION_URI);
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {
		Integer taxiID = tuple.getIntegerByField("id");
		Double distance = tuple.getDoubleByField("distance");
		Double prevDistance = idDistanceMap.put(taxiID, distance);
		if (prevDistance == null || prevDistance != distance) {
			updateDashboard(idDistanceMap.size(),
					idDistanceMap.values().stream().mapToDouble(Double::doubleValue).sum());
		}

		return true;
	}

	public void updateDashboard(Integer count, Double distance) {
		Map<String, String> map = new HashMap<>();
		map.put("taxiCount", Integer.toString(count));
		map.put("distance", Double.toString(distance));
		sendJSONPostRequest(map);
	}

}
