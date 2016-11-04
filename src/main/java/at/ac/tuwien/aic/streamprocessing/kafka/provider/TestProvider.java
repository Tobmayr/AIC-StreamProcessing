package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Sample Provider for demonstration purposes, delete after real integration.
 */
public class TestProvider implements TaxiEntryProvider {

    @Override
    public Stream<TaxiEntry> getEntries() {
        List<TaxiEntry> entries = new ArrayList<>();
        entries.add(new TaxiEntry(1, LocalDateTime.MIN, 0.0, 0.0));

        return entries.stream();
    }
}
