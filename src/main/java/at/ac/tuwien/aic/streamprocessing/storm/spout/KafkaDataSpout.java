package at.ac.tuwien.aic.streamprocessing.storm.spout;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class KafkaDataSpout extends KafkaSpout {

    public KafkaDataSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    public static KafkaDataSpout create(ZkHosts zkHosts, String topic) {
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "", "foo");
        spoutConfig.scheme = new SchemeAsMultiScheme(new TaxiEntryScheme());

        return new KafkaDataSpout(spoutConfig);
    }
}