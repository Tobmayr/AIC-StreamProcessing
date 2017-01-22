package at.ac.tuwien.aic.streamprocessing.model.serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;

/**
 * Serializer for taxi entries.
 */
public class TaxiEntrySerializer implements Serializer<TaxiEntry> {
    private final static Logger logger = LoggerFactory.getLogger(TaxiEntrySerializer.class);

    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
    }

    /**
     * Serialize taxi entry to byte[].
     *
     * @param data
     *            the entry which to serialize
     * @return the resulting byte[]
     */
    @Override
    public byte[] serialize(String topic, TaxiEntry data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayStream)) {
                objectOutputStream.writeObject(data);
            }
            return Base64.getEncoder().encode(byteArrayStream.toByteArray());
        } catch (IOException e) {
            logger.error("Failed to serialize " + data.toString(), e);
            return null;
        }
    }

    @Override
    public void close() {

    }
}
