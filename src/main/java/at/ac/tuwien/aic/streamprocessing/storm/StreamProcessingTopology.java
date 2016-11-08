package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateAverageSpeedBolt;
import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateDistanceBolt;
import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateSpeedBolt;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TestTaxiDataSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class StreamProcessingTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        // attach the test taxi data spout to the topology - no parallelism
        // NOTE: this is just to make sure we keep the data ordered. The kafka spout should have parallelism.
        //       we will probably need the trident framework inorder to achieve this.
        builder.setSpout("test-taxi-data-spout", new TestTaxiDataSpout(), 1);

        // attach the calculate speed bolt using fields grouping - parallelism of 15
        builder.setBolt("calculate-speed-bolt", new CalculateSpeedBolt(), 15).fieldsGrouping("test-taxi-data-spout", new Fields("id"));

        // attach the calculate average speed bolt using fields grouping - parallelism of 15
        builder.setBolt("calculate-average-speed-bolt", new CalculateAverageSpeedBolt(), 15).fieldsGrouping("calculate-speed-bolt", new Fields("id"));

        // attach the calculate distance bolt using fields grouping - parallelism of 15
        builder.setBolt("calculate-distance", new CalculateDistanceBolt(), 15).fieldsGrouping("test-taxi-data-spout", new Fields("id"));

        // create the default config object and set the number of threads to run (similar to setting number of workers in live cluster)
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

        // create the local cluster instance
        LocalCluster cluster = new LocalCluster();

        // submit the topology to the local cluster
        cluster.submitTopology("stream-processing", conf, builder.createTopology());

        // run the topology for 20 seconds
        Thread.sleep(20000);

        // shutdown the local cluster
        cluster.shutdown();
    }
}
