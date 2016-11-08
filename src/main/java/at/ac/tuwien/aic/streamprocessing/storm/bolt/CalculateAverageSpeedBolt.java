package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CalculateAverageSpeedBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer id = input.getIntegerByField("id");
        Double speed = input.getDoubleByField("speed");

        //TODO calculate average speed and store it to redis
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
