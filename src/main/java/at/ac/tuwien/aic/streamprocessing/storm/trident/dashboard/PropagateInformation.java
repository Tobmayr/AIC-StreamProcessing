package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.information.InformationState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class PropagateInformation extends DashboardNotifier {

    public PropagateInformation(String dashboardAddress) {
       super(dashboardAddress + Constants.PROPAGATE_INFORMATION_URI);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        InformationState information = (InformationState) tuple.get(0);
        Map<String, String> map = new HashMap<>();
        map.put("taxiCount", Integer.toString(information.getTaxiCount()));
        map.put("distance", Double.toString(information.getDistance()));
        sendJSONPostRequest(map);

        return true;
    }

}
