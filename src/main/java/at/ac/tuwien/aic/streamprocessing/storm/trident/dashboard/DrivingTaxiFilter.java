package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class DrivingTaxiFilter extends DashboardNotifier {

    private final Logger logger = LoggerFactory.getLogger(DrivingTaxiFilter.class);

    public DrivingTaxiFilter(String dashboardAddress) {
        super(dashboardAddress + Constants.NOTIFY_TAXI_STOPPED_URI);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        Double latitude = tuple.getDoubleByField("latitude");
        Double longitude = tuple.getDoubleByField("longitude");

        if (latitude == Constants.STOP_TOKEN_VALUE && longitude == Constants.STOP_TOKEN_VALUE) {
            logger.debug(String.format("Taxi with id \"%s\" has emmited a stop token", taxiId));
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            sendJSONPostRequest(map);

            return false;
        }
        return true;
    }

}
