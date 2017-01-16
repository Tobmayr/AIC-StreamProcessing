package at.ac.tuwien.aic.streamprocessing.storm.trident.operator;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;

public class DrivingTaxiFilter extends BaseFilter {
    private static final double STOP_VALUE = 360D;
    private final Logger logger = LoggerFactory.getLogger(DrivingTaxiFilter.class);
    private String dashboardAdress;

    public DrivingTaxiFilter(String dashboardAdress) {
        this.dashboardAdress = dashboardAdress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer id = tuple.getIntegerByField("id");
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");

        if (latitude == STOP_VALUE && longitude == STOP_VALUE) {
            logger.debug(String.format("Taxi with id \"%s\" has emmited a stop token", id));
            String data = toJSON(id);
            HttpUtil.sendJSONPostRequest(dashboardAdress + Config.NOTIFY_TAXI_STOPPED_URI, data);

            return false;
        }
        return true;
    }

    private String toJSON(Integer taxiId) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiId", Integer.toString(taxiId));
        return HttpUtil.toJSON(map);
    }

}
