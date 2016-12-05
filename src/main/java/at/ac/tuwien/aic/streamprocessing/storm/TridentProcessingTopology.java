package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.storm.spout.TestTaxiFixedDataSpout;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateAverageSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateDistance;
import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import at.ac.tuwien.aic.streamprocessing.storm.trident.StoreInformation;
import at.ac.tuwien.aic.streamprocessing.storm.trident.StoreInformation.OperatorType;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentProcessingTopology {

    public static StormTopology buildTopology() {
        Fields taxiFields = new Fields("id", "timestamp", "latitude", "longitude");
        Fields taxiFieldsWithSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed");
        Fields taxiFieldsWithAvgSpeed = new Fields("id", "timestamp", "latitude", "longitude", "speed", "avgSpeed");
        Fields taxiFieldsWithDistance = new Fields("id", "timestamp", "latitude", "longitude", "distance");

        TridentTopology topology = new TridentTopology();
        TestTaxiFixedDataSpout spout = new TestTaxiFixedDataSpout();
        Stream inputStream = topology.newStream("taxi", spout);

        inputStream.partitionAggregate(taxiFields, new CalculateSpeed(), taxiFieldsWithSpeed).toStream().each(taxiFieldsWithSpeed, new Debug("speed"))
                .partitionAggregate(taxiFieldsWithSpeed, new CalculateAverageSpeed(), taxiFieldsWithAvgSpeed).toStream()
                .each(taxiFieldsWithAvgSpeed, new Debug("avgSpeed")).each(taxiFieldsWithAvgSpeed, new StoreInformation(OperatorType.AVERGAGE_SPEED));

        inputStream.partitionAggregate(taxiFields, new CalculateDistance(), taxiFieldsWithDistance).toStream()
                .each(taxiFieldsWithDistance, new Debug("distance")).each(taxiFieldsWithDistance, new StoreInformation(OperatorType.DISTANCE));

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        // this method is for testing only
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(1);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, buildTopology());

        Thread.sleep(20000);

        cluster.shutdown();

        throw new RuntimeException("Exit Zookeper the Hard way");
    }

}
