package at.ac.tuwien.aic.streamprocessing.storm.bolt;

import at.ac.tuwien.aic.streamprocessing.storm.trident.CalculateSpeed;
import org.apache.storm.shade.org.joda.time.DateTime;
import org.apache.storm.shade.org.joda.time.Duration;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormat;
import org.apache.storm.shade.org.joda.time.format.DateTimeFormatter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.List;
import java.util.Map;

import static at.ac.tuwien.aic.streamprocessing.storm.bolt.Haversine.haversine;

public class WindowedCalculateSpeedBolt extends BaseWindowedBolt {

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
    public void execute(TupleWindow tw) {
        List<Tuple> tuples = tw.get();
        if (tuples.size() != 2 ) {return;}
        Tuple t1 = tuples.get(0);
        Tuple t2 = tuples.get(1);


        Integer id = t1.getIntegerByField("id");
        String timestamp = t1.getStringByField("timestamp");
        Double latitude = t1.getDoubleByField("latitude");
        Double longitude = t1.getDoubleByField("longitude");

        String lastTimestamp = t2.getStringByField("timestamp");
        Double lastLatitude = t2.getDoubleByField("latitude");
        Double lastLongitude = t2.getDoubleByField("longitude");

        Double speed = speed(timestamp, latitude, longitude, lastTimestamp, lastLatitude, lastLongitude);

        collector.emit(new Values(id, speed));
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
        return (new org.apache.storm.shade.org.joda.time.Duration(startTime, endTime)).getMillis() / 3600000.0;
    }



    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "speed"));
    }
}
