package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.storm.spout.TestTaxiDataSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Consumer;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;

public class TridentProcessingTopology {

    public static void main(String[] args) throws Exception {

        TridentTopology topology = new TridentTopology();

        Fields id = new Fields("id");
        Fields speed = new Fields("speed");

        TridentState distance = topology.newStream("taxis", new TestTaxiDataSpout())
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.out.println("XXXXX " + input.toString());
                    }
                })
                .groupBy(id)
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), speed); //CalculateSpeed

        topology.newStream("taxis-distance", new TestTaxiDataSpout())
                //.stateQuery(distance, new Fields("id"),new MapGet(),new Fields("speed"))
                .peek(new Consumer() {
                    @Override
                    public void accept(TridentTuple input) {
                        System.out.println("YYYYY" + input.getString(1));
                    }
                });


        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("stream-processing", conf, topology.build());

        Thread.sleep(20000);

        cluster.shutdown();

        throw new RuntimeException("Exit Zookeper the Hard way");
    }

}
