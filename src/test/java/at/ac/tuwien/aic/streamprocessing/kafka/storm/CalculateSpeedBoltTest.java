package at.ac.tuwien.aic.streamprocessing.kafka.storm;

import at.ac.tuwien.aic.streamprocessing.storm.bolt.CalculateSpeedBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class CalculateSpeedBoltTest {

    private CalculateSpeedBolt calculateSpeedBolt;

    @Before
    public void setUp() {
        this.calculateSpeedBolt = new CalculateSpeedBolt();
    }

    @Test
    public void shouldEmitSomethingIfTupleIsValid() {
        // given
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        Tuple correctTuple = mockTuple(1000, "2002-02-22 13:37:41", 122.0, 66.0);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        Map conf = mock(Map.class);
        calculateSpeedBolt.prepare(conf, context, collector);

        //when
        calculateSpeedBolt.execute(correctTuple);

        // then
        verify(collector).emit(any(Values.class));
    }

    @Test
    public void shouldNotEmitAnythingIfTupleIsNotValid() {
        // given
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);
        Tuple correctTuple = mockTuple(1000, "2002-02-22 13:37:41", null, 66.0);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        Map conf = mock(Map.class);
        calculateSpeedBolt.prepare(conf, context, collector);

        //when
        calculateSpeedBolt.execute(correctTuple);

        // then
        verifyZeroInteractions(collector);
    }

    @Test
    public void shouldDeclareOutputFields() {
        // given
        OutputFieldsDeclarer declarer = mock(OutputFieldsDeclarer.class);

        // when
        calculateSpeedBolt.declareOutputFields(declarer);

        // then
        verify(declarer, times(1)).declare(any(Fields.class));
    }

    private Tuple mockTuple(Integer id, String timestamp, Double latitude, Double longitude) {
        Tuple tuple = mock(Tuple.class);
        when(tuple.getIntegerByField("id")).thenReturn(id);
        when(tuple.getStringByField("timestamp")).thenReturn(timestamp);
        when(tuple.getDoubleByField("latitude")).thenReturn(latitude);
        when(tuple.getDoubleByField("longitude")).thenReturn(longitude);
        return tuple;
    }
}
