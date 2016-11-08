package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static java.util.Arrays.asList;

public class CalculateSpeedBolt extends BaseRichBolt {
    
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

        Double speed = 0.0;

        if(id != null && timestamp != null && latitude !=null && longitude !=null) {
            //TODO calculate speed
            collector.emit(asList(id, speed));
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "speed"));
    }
}