package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Interface for provides which yield a stream of TaxiEntries.
 */
public interface TaxiEntryProvider {

    /**
     * Gets a stream of taxi entries.
     *
     * @return the entries which this instance provides.
     */
    Stream<TaxiEntry> getEntries();
}
