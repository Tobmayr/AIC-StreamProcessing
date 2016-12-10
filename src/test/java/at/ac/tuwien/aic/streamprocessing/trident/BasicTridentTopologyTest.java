package at.ac.tuwien.aic.streamprocessing.trident;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.storm.trident.Haversine;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


public class BasicTridentTopologyTest extends AbstractTridentTopologyTest {

    @Test
    public void testBasic() throws Exception {
        // model a stationary taxi
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, LocalDateTime.now(), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(5), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(10), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(10);

        // three data points yield two speed + distance updates and one average speed update
        assertThat(getSpeedTupleListener().getTuples(), hasSize(2));
        assertThat(getDistanceTupleListener().getTuples(), hasSize(2));
        assertThat(getAvgSpeedTupleListener().getTuples(), hasSize(1));

        assertThat(getSpeedTupleListener().getTuples().get(0).speed, equalTo(0.0));
        assertThat(getSpeedTupleListener().getTuples().get(1).speed, equalTo(0.0));

        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(0.0));
        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(0.0));

        assertThat(getAvgSpeedTupleListener().getTuples().get(0).avgSpeed, equalTo(0.0));
    }

    @Test
    public void testMoving() throws Exception {
        // model a moving taxi
        LocalDateTime now = LocalDateTime.now();
        List<TaxiEntry> taxis = Arrays.asList(
                new TaxiEntry(1, now, 10.0, 10.0),
                new TaxiEntry(1, now.plusMinutes(60), 10.5, 10.0),
                new TaxiEntry(1, now.plusMinutes(2 * 60), 10.0, 10.0)
        );

        emitTaxis(taxis);

        wait(10);

        // three data points yield two speed + distance updates and one average speed update
        assertThat(getSpeedTupleListener().getTuples(), hasSize(2));
        assertThat(getDistanceTupleListener().getTuples(), hasSize(2));
        assertThat(getAvgSpeedTupleListener().getTuples(), hasSize(1));

        // As both trips are of equal length and take an hour each, all values should be the same
        double dist1 = Haversine.haversine(10.0, 10.0, 10.5, 10.0);
        double dist2 = Haversine.haversine(10.5, 10.0, 10.0, 10.0);

        assertThat(getSpeedTupleListener().getTuples().get(0).speed, equalTo(dist1));
        assertThat(getSpeedTupleListener().getTuples().get(1).speed, equalTo(dist2));

        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(dist1));
        assertThat(getDistanceTupleListener().getTuples().get(0).distance, equalTo(dist2));

        assertThat(getAvgSpeedTupleListener().getTuples().get(0).avgSpeed, equalTo(dist1));
    }
}
