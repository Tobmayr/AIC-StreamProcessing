package at.ac.tuwien.aic.streamprocessing.model.serialization;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

/**
 * Serializer for taxi entries.
 */
public class TaxiEntrySerializer {
    private final static Logger logger = LoggerFactory.getLogger(TaxiEntrySerializer.class);

    /**
     * Serialize taxi entry to byte[].
     *
     * @param entry the entry which to serialize
     * @return the resulting byte[]
     */
    public static byte[] serialize(TaxiEntry entry) {
        try (ByteArrayOutputStream byteArrayStream = new ByteArrayOutputStream()) {
            try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayStream)) {
                objectOutputStream.writeObject(entry);
            }
            return byteArrayStream.toByteArray();
        } catch (IOException e) {
            logger.debug("Failed to serialize " + entry.toString(), e);
            return null;
        }
    }
}
