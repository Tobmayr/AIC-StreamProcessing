package at.ac.tuwien.aic.streamprocessing.storm;

import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateAverageSpeedBolt;
import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateDistanceBolt;
import at.ac.tuwien.aic.streamprocessing.storm.bolt.WindowedCalculateSpeedBolt;
import at.ac.tuwien.aic.streamprocessing.storm.spout.TestTaxiDataSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StreamProcessingTopology {

    public static void main(String[] args) throws Exception {
        final Logger logger = LoggerFactory.getLogger(StreamProcessingTopology.class);


        TopologyBuilder builder = new TopologyBuilder();

        // attach the test taxi data spout to the topology - no parallelism
        // NOTE: this is just to make sure we keep the data ordered. The kafka spout should have parallelism.
        //       we will probably need the trident framework inorder to achieve this.
        builder.setSpout("test-taxi-data-spout", new TestTaxiDataSpout(), 1);

        // attach the calculate speed bolt using fields grouping - parallelism of 15
        //builder.setBolt("calculate-speed-bolt", new CalculateSpeedBolt(), 15).fieldsGrouping("test-taxi-data-spout", new Fields("id"));
        builder.setBolt("w-calculate-speed-bolt", new WindowedCalculateSpeedBolt().withWindow(new BaseWindowedBolt.Count(2)), 15)
                .fieldsGrouping("test-taxi-data-spout", new Fields("id"));

        // attach the calculate average speed bolt using fields grouping - parallelism of 15
        builder.setBolt("calculate-average-speed-bolt", new CalculateAverageSpeedBolt(), 15).fieldsGrouping("w-calculate-speed-bolt", new Fields("id"));

        // attach the calculate distance bolt using fields grouping - parallelism of 15
        builder.setBolt("calculate-distance", new CalculateDistanceBolt(), 15).fieldsGrouping("test-taxi-data-spout", new Fields("id"));

        // create the default config object and set the number of threads to run (similar to setting number of workers in live cluster)
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(3);

//        if(args[0].equals("cluster")) {
//            System.setProperty("storm.jar", "./apache-storm-1.0.2/lib/storm-core-1.0.2.jar");
            StormSubmitter.submitTopology("taxicab-0_0_1",conf,builder.createTopology());
            logger.debug("submitted Topology");

//            LocalCluster cluster = new LocalCluster();
//
//            // submit the topology to the local cluster
//            cluster.submitTopology("stream-processing", conf, builder.createTopology());
//
//            // run the topology for 20 seconds
//            Thread.sleep(20000);
//
//            // shutdown the local cluster
//            cluster.shutdown();
//        }
    }
}
