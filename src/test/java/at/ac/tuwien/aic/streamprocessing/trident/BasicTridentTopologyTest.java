package at.ac.tuwien.aic.streamprocessing.trident;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.StateFactory;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.RedisState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedState;
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
        double dist1 = Haversine.calculateDistanceBetween(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.calculateDistanceBetween(10.5, 10.0, 10.0, 10.0);

        assertThat(collectSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(1), contains(0.0, dist1 / 2.0, (2 * dist1) / 3.0));
        assertThat(collectDistance(1), contains(0.0, dist1, dist1 + dist2));
    }

    @Test
    public void test_simpleMoving_inBatches_yieldsCorrectValues() throws Exception {
        // model a moving taxi
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0)
        );

        emitTaxis(taxis);
        wait(15);

        double dist = Haversine.calculateDistanceBetween(10.0, 10.0, 10.5, 10.0);
        assertThat(collectSpeed(1), contains(0.0, dist, dist));
        assertThat(collectAverageSpeed(1), contains(0.0, dist / 2.0, (2 * dist) / 3.0));
        assertThat(collectDistance(1), contains(0.0, dist, 2*dist));

        taxis = Arrays.asList(
                new TaxiEntry(1, now.plusMinutes(3 * 60), 10.5, 10.0)
        );

        emitTaxis(taxis);
        wait(15);

        assertThat(collectSpeed(1), contains(0.0, dist, dist, dist));
        assertThat(collectDistance(1), contains(0.0, dist, 2*dist, 3*dist));
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
        double dist1 = Haversine.calculateDistanceBetween(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.calculateDistanceBetween(10.5, 10.0, 10.0, 10.0);

        assertThat(collectSpeed(1), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(1), contains(0.0, dist1 / 2.0, (2 * dist2) / 3.0));
        assertThat(collectDistance(1), contains(0.0, dist1, dist1 + dist2));

        assertThat(collectSpeed(2), contains(0.0, dist1, dist2));
        assertThat(collectAverageSpeed(2), contains(0.0, dist1 / 2.0, (2 * dist2) / 3.0));
        assertThat(collectDistance(2), contains(0.0, dist1, dist1 + dist2));
    }

    @Test
    public void test_withState_yieldsCorrectValues() throws Exception {
        LocalDateTime now = LocalDateTime.now();

        RedisState distanceState = StateFactory.createDistanceStateFactory(getTopology().getRedisHost(), getTopology().getRedisPort()).create();
        distanceState.setAll(Arrays.asList(1), Arrays.asList(new DistanceState(10.5, 10.0, 42.0)));

        RedisState speedState = StateFactory.createSpeedStateFactory(getTopology().getRedisHost(), getTopology().getRedisPort()).create();
        speedState.setAll(Arrays.asList(1), Arrays.asList(new SpeedState(Timestamp.toString(now.minusMinutes(60)), 10.5, 10.0, 50.0)));

        RedisState avgSpeedState = StateFactory.createAverageSpeedStateFactory(getTopology().getRedisHost(), getTopology().getRedisPort()).create();
        avgSpeedState.setAll(Arrays.asList(1), Arrays.asList(new AverageSpeedState(1, 50.0)));

        wait(5);

        // model a moving taxi
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0)
        );

        emitTaxis(taxis);

        wait(15);

        double initialDistance = 42.0;
        double initialAvgSpeed = 50.0;
        double dist1 = Haversine.calculateDistanceBetween(10.5, 10.0, 10.0, 10.0);
        double dist2 = Haversine.calculateDistanceBetween(10.0, 10.0, 10.5, 10.0);

        assertThat(collectSpeed(1), contains(dist1, dist2));
        assertThat(collectAverageSpeed(1), contains((initialAvgSpeed + dist1) / 2.0, (initialAvgSpeed + dist1 + dist2) / 3.0));
        assertThat(collectDistance(1), contains(initialDistance + dist1, initialDistance + dist1 + dist2));
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
