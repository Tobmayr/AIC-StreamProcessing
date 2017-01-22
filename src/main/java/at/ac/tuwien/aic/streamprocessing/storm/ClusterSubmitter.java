package at.ac.tuwien.aic.streamprocessing.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kern on 1/21/17.
 */
public class ClusterSubmitter extends TridentProcessingTopology {

    public ClusterSubmitter(String topic, String redisHost, int redisPort, String dashboardAdress) {
        super(topic, redisHost, redisPort, dashboardAdress);
    }

    public static void main(String[] args) throws Exception {
        BaseFilter speedListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };
        BaseFilter avgSpeedListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };
        BaseFilter distanceListener = new BaseFilter() {
            @Override
            public boolean isKeep(TridentTuple tuple) {
                return true;
            }
        };
        TridentProcessingTopology topology = null;
        try {
            topology = createWithListeners(speedListener, avgSpeedListener, distanceListener);
            topology.submitCluster();
        } finally {
            if (topology != null) {
                Runtime.getRuntime().addShutdownHook(new Thread(topology::stop));
            }

        }

    }
}
