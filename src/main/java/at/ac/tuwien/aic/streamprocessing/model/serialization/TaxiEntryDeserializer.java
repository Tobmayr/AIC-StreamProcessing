package at.ac.tuwien.aic.streamprocessing.model.serialization;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * Deserializer for taxi entries.
 */
public class TaxiEntryDeserializer {
    private final static Logger logger = LoggerFactory.getLogger(TaxiEntryDeserializer.class);

    /**
     * Deserialize a taxi entry.
     *
     * @param bytes the bytes to deserialize.
     * @return the corresponding taxi entry
     */
    public static TaxiEntry deserialize(byte[] bytes) {
        try (ByteArrayInputStream byteArrayStream = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayStream)) {
                return (TaxiEntry) objectInputStream.readObject();
            } catch (ClassNotFoundException e) {
                logger.debug("Failed to deserialize TaxiEntry", e);
                return null;
            }
        } catch (IOException e) {
            logger.debug("Failed to deserialize TaxiEntry", e);
            return null;
        }
    }
}
