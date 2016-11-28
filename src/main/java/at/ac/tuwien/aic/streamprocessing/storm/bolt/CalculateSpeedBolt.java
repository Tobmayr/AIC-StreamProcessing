package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static at.ac.tuwien.aic.streamprocessing.storm.bolt.Haversine.haversine;

public class CalculateSpeedBolt extends BaseRichBolt {

    /**
     * The _Calculate speed_ operator calculates the speed between two successive locations for each taxi, whereas the
     * distance between two locations can be derived by the Haversine formula{4}. This operator represents a stateful
     * operator because it is required to always remember the last location of the taxi to calculate the current
     * speed[1]
     */

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

        // TODO get last position instead of current
        String lastTimestamp = "2008-02-02 13:37:00";
        Double lastLatitude = 116.44925;
        Double lastLongitude = 39.97968;

        Double speed = speed(timestamp, latitude, longitude, lastTimestamp, lastLatitude, lastLongitude);

        if (speed != null) {
            collector.emit(new Values(id, speed));
        }
    }

    static Double speed(String timestamp, Double latitude, Double longitude, String lastTimestamp, Double lastLatitude, Double lastLongitude) {
        Double distance; // in km
        Double time; // in hours
        if (timestamp != null && latitude != null && longitude != null) {
            distance = haversine(lastLatitude, lastLongitude, latitude, longitude);
            time = time(lastTimestamp, timestamp);
            return distance / time;

        }
        return null;
    }


    static Double time(String startTimestamp, String endTimestamp) {
        DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime startTime = fmt.parseDateTime(startTimestamp);
        DateTime endTime = fmt.parseDateTime(endTimestamp);
        return (new Duration(startTime, endTime)).getMillis() / 3600000.0;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "speed"));
    }
}
