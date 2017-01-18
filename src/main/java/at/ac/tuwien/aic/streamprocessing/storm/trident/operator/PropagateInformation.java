package at.ac.tuwien.aic.streamprocessing.storm.trident.operator;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.information.InformationState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Config;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HttpUtil;

public class PropagateInformation extends BaseFilter {

    private final String dashboardAdress;

    public PropagateInformation(String dashboardAdress) {
        this.dashboardAdress = dashboardAdress;
    }

    @Override
    public boolean isKeep(TridentTuple tuple) {
        InformationState information = (InformationState) tuple.get(0);
        String data = toJSON(information);
        HttpUtil.sendJSONPostRequest(dashboardAdress + Config.PROPAGATE_INFORMATION_URI, data);

        return true;
    }

    private String toJSON(InformationState information) {
        Map<String, String> map = new HashMap<>();
        map.put("taxiCount", Integer.toString(information.getTaxiCount()));
        map.put("distance", Double.toString(information.getDistance()));
        return HttpUtil.toJSON(map);
    }
}
