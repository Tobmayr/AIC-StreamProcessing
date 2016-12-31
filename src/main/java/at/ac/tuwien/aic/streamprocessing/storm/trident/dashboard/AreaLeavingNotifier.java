package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;

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
        if (distance >= Config.WARNING_DISTANCE) {
            String data = toJSON(taxiId, distance);
            HttpUtil.sendJSONPostRequest(dashboardAdress + Config.NOTIFY_AREA_VIOLATION_URI, data);
        }
        return true;
    }

    private String toJSON(Integer taxiId, Double distance) {
        return String.format("[{\"taxiId\":\"%s\",\"distance\":\"%s\"}]", taxiId, distance);
    }

}
