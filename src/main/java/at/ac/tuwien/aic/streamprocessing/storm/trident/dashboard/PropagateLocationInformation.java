package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;

public class PropagateLocationInformation extends BaseFilter {
    private final Logger logger = LoggerFactory.getLogger(PropagateLocationInformation.class);
    private String dashboardAdress;

    public PropagateLocationInformation(String dashboardAdress) {
        super();
        this.dashboardAdress = dashboardAdress;

    }

    @Override
    public boolean isKeep(TridentTuple tuple) {

        Integer taxiId = tuple.getIntegerByField("id");
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");
        String data = toJSON(taxiId, latitude, longitude);
        HttpUtil.sendJSONPostRequest(dashboardAdress + Config.PROPAGATE_LOCATION_URI, data);
        return true;
    }

    private String toJSON(Integer taxiId, Double latitude, Double longitude) {
        return String.format("[{\"taxiId\":\"%s\",\"latitude\":\"%s\",\"longitude\":\"%s\"}]", taxiId, latitude, longitude);
    }
}
