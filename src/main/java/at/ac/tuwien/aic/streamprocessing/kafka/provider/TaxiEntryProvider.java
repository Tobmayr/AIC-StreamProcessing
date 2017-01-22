package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import java.util.stream.Stream;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;

/**
 * Interface for providers which yield a stream of TaxiEntries.
 */
public interface TaxiEntryProvider {

    /**
     * Gets a stream of taxi entries.
     *
     * @return the entries which this instance provides.
     */
    Stream<TaxiEntry> getEntries();
}
