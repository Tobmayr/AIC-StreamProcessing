package at.ac.tuwien.aic.streamprocessing.storm.spout;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import org.apache.kafka.common.utils.Utils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Random;

import static java.util.Arrays.asList;

/**
 * Generates random taxi data.
 * NOTE: although the data is random, it is always produced in an ordered fashion (by increasing the last timestamp)
 */
public class TestTaxiDataSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random;
    private DateTimeFormatter dateTimeFormatter;
    private LocalDateTime lastTimestamp;
    private TaxiEntry taxiData[];

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        this.lastTimestamp = LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter);
        this.taxiData = new TaxiEntry[] {
                new TaxiEntry(10239, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.44925,39.97968),
                new TaxiEntry(10340, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter), 116.34158,39.90602),
                new TaxiEntry(10124, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.47564,39.96593),
                new TaxiEntry(10044, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.40824,39.98746),
                new TaxiEntry(5085, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45512,39.95064),
                new TaxiEntry(5047, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.34764,39.87314),
                new TaxiEntry(5024, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.45182,39.91064),
                new TaxiEntry(10247, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.35117,39.90763),
                new TaxiEntry(10168, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.56588,40.07176),
                new TaxiEntry(10263, LocalDateTime.parse("2008-02-02 13:37:00", dateTimeFormatter),116.40133,39.89942)
        };
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);
        TaxiEntry taxiEntry = taxiData[random.nextInt(taxiData.length)];

        //simulate time change from 0 to 10 minutes
        lastTimestamp = lastTimestamp.plusMinutes(random.nextInt(10));
        taxiEntry.setTimestamp(lastTimestamp);

        //change position of the taxi
        taxiEntry.setLatitude(taxiEntry.getLatitude() + (double)random.nextInt(5) / 1000.0);
        taxiEntry.setLongitude(taxiEntry.getLongitude() + (double)random.nextInt(5) / 1000.0);

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