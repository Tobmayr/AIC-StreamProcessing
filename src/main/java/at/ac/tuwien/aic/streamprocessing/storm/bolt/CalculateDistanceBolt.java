package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static at.ac.tuwien.aic.streamprocessing.storm.bolt.Haversine.haversine;

public class CalculateDistanceBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        Integer id = input.getIntegerByField("id");
        Double latitude = input.getDoubleByField("latitude");
        Double longitude = input.getDoubleByField("longitude");

        // TODO get last location from somewhere (sliding window, trident or redis)
        Double lastLatitude = 0.0;
        Double lastLongitude = 0.0;

        Double distance = haversine(lastLatitude, lastLongitude, latitude, longitude);

        collector.emit(new Values(id, distance));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //TODO
    }
}
