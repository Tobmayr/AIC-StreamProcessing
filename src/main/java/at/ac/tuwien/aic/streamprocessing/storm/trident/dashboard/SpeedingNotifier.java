package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class SpeedingNotifier extends DashboardNotifier {

    public SpeedingNotifier(String dashboardAddress) {
        super(dashboardAddress + Constants.NOTIFY_SPEEDING_INCIDENT_URI);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Double speed = tuple.getDoubleByField("speed");
        if (speed >= Constants.SPEED_LIMIT) {
            Integer taxiId = tuple.getIntegerByField("id");
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            map.put("speed", Double.toString(speed));
            sendJSONPostRequest(map);
        }
        return true;
    }

  

}
