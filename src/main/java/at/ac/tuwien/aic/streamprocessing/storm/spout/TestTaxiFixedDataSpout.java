package at.ac.tuwien.aic.streamprocessing.storm.spout;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Time;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static java.util.Arrays.asList;

/**
 * Generates random taxi data.
 * NOTE: although the data is random, it is always produced in an ordered fashion (by increasing the last timestamp)
 */
public class TestTaxiFixedDataSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private DateTimeFormatter dateTimeFormatter;
    private LocalDateTime lastTimestamp;
    private TaxiEntry taxiData[];
    private int currentTaxi;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        this.lastTimestamp = LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter);
        this.taxiData = new TaxiEntry[] {
                new TaxiEntry(10815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45512,39.95064),
                new TaxiEntry(20815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.34764,39.87314),
                new TaxiEntry(30815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45182,39.91064),
                new TaxiEntry(10815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45512,39.95064),
                new TaxiEntry(20815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.35764,39.87314),
                new TaxiEntry(30815, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45182,39.93064)
        };
        this.currentTaxi = 0;
    }

    @Override
    public void nextTuple() {
        try {
            Time.sleep(100);
        } catch (InterruptedException e) {
            //e.printStackTrace();
        }
        TaxiEntry taxiEntry = this.taxiData[this.currentTaxi];
        this.currentTaxi = (this.currentTaxi + 1) % (this.taxiData.length);

        lastTimestamp = lastTimestamp.plusMinutes(10);
        taxiEntry.setTimestamp(lastTimestamp);

        collector.emit(asList(
                taxiEntry.getTaxiId(),
                taxiEntry.getTimestamp().format(dateTimeFormatter),
                taxiEntry.getLatitude(),
                taxiEntry.getLongitude()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "timestamp", "latitude", "longitude"));
    }

}
