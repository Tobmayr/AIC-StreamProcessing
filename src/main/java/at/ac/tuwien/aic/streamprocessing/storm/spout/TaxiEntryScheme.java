package at.ac.tuwien.aic.streamprocessing.storm.spout;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntryDeserializer;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class TaxiEntryScheme implements Scheme {

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        TaxiEntry entry = TaxiEntryDeserializer.deserialize(ser.array());
        System.out.println("Got: " + entry); // TODO: remove, only for debug purposes
        return Collections.singletonList(entry);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "timestamp", "latitude", "longitude");
    }
}
