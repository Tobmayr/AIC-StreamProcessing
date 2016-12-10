package at.ac.tuwien.aic.streamprocessing.trident;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
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
        List<TaxiEntry> expected = Arrays.asList(
                new TaxiEntry(1, LocalDateTime.now(), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(5), 10.0, 10.0),
                new TaxiEntry(1, LocalDateTime.now().plusMinutes(10), 10.0, 10.0)
        );

        emitTaxis(expected);

        wait(10);

        // three data points yield two speed + distance updates and one average speed update
        assertThat(getSpeedHook().getTuples(), hasSize(2));
        assertThat(getDistanceHook().getTuples(), hasSize(2));
        assertThat(getAvgSpeedHook().getTuples(), hasSize(1));

        assertThat(getSpeedHook().getTuples().get(0).speed, equalTo(0.0));
        assertThat(getSpeedHook().getTuples().get(1).speed, equalTo(0.0));

        assertThat(getDistanceHook().getTuples().get(0).distance, equalTo(0.0));
        assertThat(getDistanceHook().getTuples().get(0).distance, equalTo(0.0));

        assertThat(getAvgSpeedHook().getTuples().get(0).avgSpeed, equalTo(0.0));
    }
}
