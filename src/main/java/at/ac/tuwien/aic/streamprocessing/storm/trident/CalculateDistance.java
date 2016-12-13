package at.ac.tuwien.aic.streamprocessing.storm.trident;

import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class CalculateDistance extends LastState<CalculateDistance.TaxiDistance>{

    private final Logger logger = LoggerFactory.getLogger(CalculateDistance.class);

    class TaxiDistance {
        String timestamp;
        Double latitude;
        Double longitude;
        Double traveled;
    }

    protected TaxiDistance calculate(TridentTuple newTuple, TaxiDistance taxiDistance, TridentCollector collector) {
        Integer id = newTuple.getIntegerByField("id"); //test
        String timestamp = newTuple.getStringByField("timestamp");

        TaxiDistance td = new TaxiDistance();
        td.latitude = newTuple.getDoubleByField("latitude");
        td.longitude = newTuple.getDoubleByField("longitude");
        td.timestamp = timestamp;

        if (taxiDistance == null) {
            td.traveled = 0d;
        } else {
            LocalDateTime oldTime = Timestamp.parse(taxiDistance.timestamp);
            LocalDateTime newTime = Timestamp.parse(td.timestamp);

            Double distance;

            if (oldTime.isAfter(newTime) || oldTime.isEqual(newTime)) {
                logger.debug("Old tuple is not older than new one!");

                // since it is not meaningful to compute the distance in this case, just use a default value of 0.0
                distance = 0.0;
            } else {
                distance = this.distance(newTuple, taxiDistance.latitude, taxiDistance.longitude); // in km
            }

            td.traveled = taxiDistance.traveled + distance;
            collector.emit(new Values(id, timestamp, td.latitude, td.longitude, td.traveled));
            logger.debug("(distance): [" + id + ", " + timestamp + ", " + td.latitude + ", " + td.longitude + ", " + td.traveled + "]");
        }

        return td;
    }

}
