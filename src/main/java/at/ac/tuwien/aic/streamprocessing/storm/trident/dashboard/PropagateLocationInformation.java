package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.Arrays;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.DashboardConstants;
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
        Gson gson = new Gson();
        Integer id = tuple.getIntegerByField("id");
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");
        TaxiEntry entry = new TaxiEntry(id, null, latitude, longitude);
        String data = gson.toJson(Arrays.asList(entry));
        logger.info("JSON: " + data);
        HttpUtil.sendJSONPostRequest(dashboardAdress + DashboardConstants.PROPAGATE_LOCATION_URI, data);
        return true;
    }
}
