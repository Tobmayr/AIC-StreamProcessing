package at.ac.tuwien.aic.streamprocessing.storm.spout;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.serialization.TaxiEntryDeserializer;
import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.tuple.TaxiFields;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class TaxiEntryKeyValueScheme implements KeyValueScheme {

    private final Logger logger = LoggerFactory.getLogger(TaxiEntryKeyValueScheme.class);

    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        return deserialize(value);
    }

    @Override
    public List<Object> deserialize(ByteBuffer ser) {
        String valueString = StringScheme.deserializeString(ser);
        TaxiEntry entry =  TaxiEntryDeserializer.deserialize(valueString.getBytes());

        if (entry == null) {
            // failed to deserialize
            return null;
        }

        logger.debug("Emitting ({}, {}, {}, {})",
                entry.getTaxiId(),
                Timestamp.toString(entry.getTimestamp()),
                entry.getLatitude(),
                entry.getLongitude());

        return new Values(
                entry.getTaxiId(),
                Timestamp.toString(entry.getTimestamp()),
                entry.getLatitude(),
                entry.getLongitude()
        );
    }

    @Override
    public Fields getOutputFields() {
        return TaxiFields.BASE_FIELDS;
    }
}
