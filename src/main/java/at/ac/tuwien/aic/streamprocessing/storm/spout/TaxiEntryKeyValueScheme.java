package at.ac.tuwien.aic.streamprocessing.storm.spout;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntryDeserializer;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

public class TaxiEntryKeyValueScheme implements KeyValueScheme {

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        return deserialize(value);
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        //TODO we should not deserialize twice, don't really know why this works lol
        String valueString = StringScheme.deserializeString(ser);
        TaxiEntry entry =  TaxiEntryDeserializer.deserialize(valueString.getBytes());
        System.out.println("deserialized " + entry);
        return new Values(entry.getTaxiId(), entry.getTimestamp().toString(), entry.getLatitude(), entry.getLongitude());
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("id", "timestamp", "latitude", "longitude");
    }

}
