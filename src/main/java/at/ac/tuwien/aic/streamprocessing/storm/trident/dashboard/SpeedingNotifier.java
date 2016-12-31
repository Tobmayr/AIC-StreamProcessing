package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.DashboardConstants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class SpeedingNotifier extends BaseFilter {
    private static final Double SPEED_LIMIT = new Double(50);
    private String dashboardAdress;

    public SpeedingNotifier(String dashbaordAdress) {
        this.dashboardAdress = dashbaordAdress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Double speed = tuple.getDoubleByField("speed");
        if (speed >= SPEED_LIMIT) {
            Integer taxiId = tuple.getIntegerByField("id");
            String data = toJSON(taxiId, speed);
            HttpUtil.sendJSONPostRequest(dashboardAdress + DashboardConstants.NOTIFY_SPEEDING_INCIDENT_URI, data);
        }
        return true;
    }


    private String toJSON(Integer taxiId, Double speed) {
        return String.format("[{\"taxiId\":\"%s\",\"speed\":\"%s\"}]", taxiId, speed);
    }


}
