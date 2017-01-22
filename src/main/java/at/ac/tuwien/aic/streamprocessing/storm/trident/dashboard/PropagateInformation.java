package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.Information;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;

public class PropagateInformation extends DashboardNotifier {

    private Information previousInformation = new Information(0, 0D);

    public PropagateInformation(String dashboardAddress) {
        super(dashboardAddress + Constants.PROPAGATE_INFORMATION_URI);
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        Information information = (Information) tuple.get(0);
        if (!previousInformation.equals(information)) {
            updateDashboard(information);
            previousInformation = information;
        }
        return true;
    }

    public void updateDashboard(Information information) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiCount", Integer.toString(information.getTaxiCount()));
        map.put("distance", Double.toString(information.getOverallDistance()));
        sendJSONPostRequest(map);
    }

}
