package at.ac.tuwien.aic.streamprocessing.storm.trident.operator;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class AreaLeavingNotifier extends BaseFilter {

    private final String dashboardAdress;

    public AreaLeavingNotifier(String dashboardAdress) {
        this.dashboardAdress = dashboardAdress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        Double centerLat = Config.ALLOWED_AREA_CENTER_LAT;
        Double centerLong = Config.ALLOWED_AREA_CENTER_LONG;
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");
        Double distance = Haversine.calculateDistanceBetween(centerLat, centerLong, latitude, longitude);

        if (distance >= Config.WARNING_DISTANCE && distance < Config.BORDER_DISTANCE) {
            String data = toJSON(taxiId, distance);
            HttpUtil.sendJSONPostRequest(dashboardAdress + Config.NOTIFY_AREA_VIOLATION_URI, data);
        } else if (distance > Config.BORDER_DISTANCE){
            return false;
        }
        return true;
    }

    private String toJSON(Integer taxiId, Double distance) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiId", Integer.toString(taxiId));
        map.put("distance", Double.toString(distance));
        return HttpUtil.toJSON(map);
    }

}
