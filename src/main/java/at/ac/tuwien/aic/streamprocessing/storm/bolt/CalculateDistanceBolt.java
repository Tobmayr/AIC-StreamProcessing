package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class CalculateDistanceBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer id = input.getIntegerByField("id");
        String timestamp = input.getStringByField("timestamp");
        Double latitude = input.getDoubleByField("latitude");
        Double longitude = input.getDoubleByField("longitude");

        //TODO calculate overall distance for a taxi and store it to redis
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //TODO
    }
}
