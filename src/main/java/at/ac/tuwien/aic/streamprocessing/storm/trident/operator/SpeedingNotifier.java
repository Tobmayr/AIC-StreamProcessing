package at.ac.tuwien.aic.streamprocessing.storm.trident.operator;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class SpeedingNotifier extends BaseFilter {

    private String dashboardAdress;

    public SpeedingNotifier(String dashbaordAdress) {
        this.dashboardAdress = dashbaordAdress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Double speed = tuple.getDoubleByField("speed");
        if (speed >= Config.SPEED_LIMIT) {
            Integer taxiId = tuple.getIntegerByField("id");
            String data = toJSON(taxiId, speed);
            HttpUtil.sendJSONPostRequest(dashboardAdress + Config.NOTIFY_SPEEDING_INCIDENT_URI, data);
        }
        return true;
    }

    private String toJSON(Integer taxiId, Double speed) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiId", Integer.toString(taxiId));
        map.put("speed", Double.toString(speed));
        return HttpUtil.toJSON(map);
    }

}