package at.ac.tuwien.aic.streamprocessing.trident;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.storm.trident.Haversine;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;


public class BasicTridentTopologyTest extends AbstractTridentTopologyTest {

    @Test
    public void test_stationaryTaxi_yieldCorrectValues() throws Exception {
        // model a stationary taxi
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, LocalDateTime.now(), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(5), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(10), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(15);

        assertThat(collectSpeed(1), contains(0.0, 0.0, 0.0));
        assertThat(collectAverageSpeed(1), contains(0.0, 0.0, 0.0));
        assertThat(collectDistance(1), contains(0.0, 0.0, 0.0));
    }

    @Test
    public void test_simpleMoving_yieldsCorrectValues() throws Exception {
        // model a moving taxi
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(15);

        // As both trips are of equal length and take an hour each, all values should be the same
        double dist1 = Haversine.haversine(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.haversine(10.5, 10.0, 10.0, 10.0);

        assertThat(collectSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectDistance(1), contains(0.0, dist1, dist1 + dist2));
    }

    @Test
    public void test_multipleMoving_yieldsCorrectValues() throws Exception {
        // model a moving taxi
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(2, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(2, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0),
                new TaxiEntry(2, now.plusMinutes(2 * 60), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(15);

        // As both trips are of equal length and take an hour each, all values should be the same
        double dist1 = Haversine.haversine(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.haversine(10.5, 10.0, 10.0, 10.0);

        assertThat(collectSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectDistance(1), contains(0.0, dist1, dist1 + dist2));

        assertThat(collectSpeed(2), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(2), contains(0.0, dist1, dist2));
        assertThat(collectDistance(2), contains(0.0, dist1, dist1 + dist2));
    }

    @Test
    @Ignore
    public void test_multipleEntriesAtSameTime_yieldsZeroValues() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now, 10.5, 10.0),
                new TaxiEntry(1, now, 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(10);

        // three data points yield three updates
        assertThat(getSpeedTupleListener().getTuples(), hasSize(3));
        assertThat(getDistanceTupleListener().getTuples(), hasSize(3));
        assertThat(getAvgSpeedTupleListener().getTuples(), hasSize(3));

        // As both trips are of equal length and take an hour each, all values should be the same
        double dist1 = Haversine.haversine(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.haversine(10.5, 10.0, 10.0, 10.0);

        assertThat(getSpeedTupleListener().getTuples().get(0).speed, equalTo(0.0));
        assertThat(getSpeedTupleListener().getTuples().get(1).speed, equalTo(0.0));
        assertThat(getSpeedTupleListener().getTuples().get(2).speed, equalTo(0.0));

        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(0.0));
        assertThat(getDistanceTupleListener().getTuples().get(1).distance, equalTo(0.0));
        assertThat(getDistanceTupleListener().getTuples().get(2).distance, equalTo(0.0));

        assertThat(getAvgSpeedTupleListener().getTuples().get(0).avgSpeed, equalTo(0.0));
        assertThat(getAvgSpeedTupleListener().getTuples().get(1).avgSpeed, equalTo(0.0));
        assertThat(getAvgSpeedTupleListener().getTuples().get(2).avgSpeed, equalTo(0.0));
    }

    @Test
    @Ignore
    public void test_multipleEntriesAtSameTimeThenDisparateEntries_yieldsMeaningfulValues() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now, 10.5, 10.0),
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(10);

        assertThat(getSpeedTupleListener().getTuples(), hasSize(4));
        assertThat(getDistanceTupleListener().getTuples(), hasSize(4));
        assertThat(getAvgSpeedTupleListener().getTuples(), hasSize(3));

        double dist = Haversine.haversine(10.0, 10.0, 10.5, 10.0);

        // Make sure that initial inconsistent values (i.e. multiple entries at the same time)
        // do not trap the calculations of later, meaningful entries
        assertThat(getSpeedTupleListener().getTuples().get(0).speed, equalTo(0.0));
        assertThat(getSpeedTupleListener().getTuples().get(1).speed, equalTo(0.0));
        assertThat(getSpeedTupleListener().getTuples().get(2).speed, equalTo(dist));
        assertThat(getSpeedTupleListener().getTuples().get(3).speed, equalTo(dist));

        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(0.0));
        assertThat(getDistanceTupleListener().getTuples().get(1).distance, equalTo(0.0));
        assertThat(getDistanceTupleListener().getTuples().get(2).distance, equalTo(dist));
        assertThat(getDistanceTupleListener().getTuples().get(3).distance, equalTo(dist + dist));

        assertThat(getAvgSpeedTupleListener().getTuples().get(0).avgSpeed, equalTo(0.0));
        assertThat(getAvgSpeedTupleListener().getTuples().get(1).avgSpeed, equalTo(dist));
        assertThat(getAvgSpeedTupleListener().getTuples().get(2).avgSpeed, equalTo(dist));
    }

    @Test
    @Ignore
    public void test_multipleEntriesAtSameTime_yieldsMeaningfulValues() throws Exception {
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 15.0, 15.0)
        );

        emitTaxis(taxis);

        wait(10);

        // four data points yield three speed + distance updates and two average speed update
        assertThat(getSpeedTupleListener().getTuples(), hasSize(3));
        assertThat(getDistanceTupleListener().getTuples(), hasSize(3));
        assertThat(getAvgSpeedTupleListener().getTuples(), hasSize(2));

        // Important that last entry is ignored as there are two entries at the same time
        // Speed of the tuple is zero and the distance does not change and the average speed keeps the previous value
        double dist1 = Haversine.haversine(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.haversine(10.5, 10.0, 10.0, 10.0);

        assertThat(getSpeedTupleListener().getTuples().get(0).speed, equalTo(dist1));
        assertThat(getSpeedTupleListener().getTuples().get(1).speed, equalTo(dist2));
        assertThat(getSpeedTupleListener().getTuples().get(2).speed, equalTo(0.0));

        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(dist1));
        assertThat(getDistanceTupleListener().getTuples().get(1).distance, equalTo(dist1 + dist2));
        assertThat(getDistanceTupleListener().getTuples().get(2).distance, equalTo(dist1 + dist2));

        assertThat(getAvgSpeedTupleListener().getTuples().get(0).avgSpeed, equalTo(dist1));
        assertThat(getAvgSpeedTupleListener().getTuples().get(1).avgSpeed, equalTo(dist2));
    }

    private List<Double> collectSpeed(int taxiId) {
        return getSpeedTupleListener().getTuples().stream()
                .filter(t -> t.id == taxiId)
                .map(t -> t.speed)
                .collect(Collectors.toList());
    }

    private List<Double> collectAverageSpeed(int taxiId) {
        return getAvgSpeedTupleListener().getTuples().stream()
                .filter(t -> t.id == taxiId)
                .map(t -> t.avgSpeed)
                .collect(Collectors.toList());
    }

    private List<Double> collectDistance(int taxiId) {
        return getDistanceTupleListener().getTuples().stream()
                .filter(t -> t.id == taxiId)
                .map(t -> t.distance)
                .collect(Collectors.toList());
    }
}
