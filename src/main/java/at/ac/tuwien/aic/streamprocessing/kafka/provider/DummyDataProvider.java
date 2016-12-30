package at.ac.tuwien.aic.streamprocessing.kafka.provider;

import at.ac.tuwien.aic.streamprocessing.model.TaxiEntry;
import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.HTTPUtil;

import com.google.gson.Gson;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.storm.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author tobias
 *
 */
public class DummyDataProvider {

    private static final Logger logger = LoggerFactory.getLogger(DataProvider.class);
    private static final LocalDateTime REFERENCE_START_TIME = Timestamp.parse("2008-02-02 13:30:45");

    public DummyDataProvider() {
    }

    private void provide(Reader reader) {

        try {
            CSVParser csv = CSVFormat.EXCEL.parse(reader);

            LocalDateTime currentBatchStart = REFERENCE_START_TIME;
            LocalDateTime nextBatchStart;
            TaxiEntry last = null;

            Iterator<CSVRecord> recordIterator = csv.iterator();
            Gson gson = new Gson();

            while (recordIterator.hasNext()) {
                Batch batch = getNextBatch(recordIterator, last, currentBatchStart);

                // send json post request
                HTTPUtil.sendJSONPostRequest("http://127.0.0.1:3000/add", gson.toJson(batch.entries));

                if (batch.last != null) {
                    nextBatchStart = batch.last.getTimestamp();

                    currentBatchStart = nextBatchStart;
                    last = batch.last;

                    // simulate waiting for next entry
                    try {
                        Time.sleep(200);
                    } catch (InterruptedException e) {
                        logger.error("Thread was interrupted", e);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Filed reading the file!", e);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Batch getNextBatch(Iterator<CSVRecord> recordIterator, TaxiEntry startEntry, LocalDateTime until) {
        List<TaxiEntry> entries = new ArrayList<>();

        if (startEntry != null) {
            entries.add(startEntry);
        }

        while (recordIterator.hasNext()) {
            CSVRecord record = recordIterator.next();
            TaxiEntry entry = parseCsvRecord(record);

            if (entry == null) {
                // malformed record, ignore
                continue;
            }

            if (entry.getTimestamp().isAfter(until)) {
                // found end of current batch
                return new Batch(entries, entry);
            }

            long sameEntryCount = entries.stream().filter(e -> e.getTaxiId() == entry.getTaxiId()).filter(e -> e.getTimestamp().isEqual(entry.getTimestamp()))
                    .count();

            if (sameEntryCount == 0) {
                entries.add(entry);
            } else {
                // duplicate entries for one taxi at the same moment
                // disregard it as it will computing meaningful values impossible
                logger.debug("Filtered same-time entry for taxi " + entry.getTaxiId() + ": " + entry.toString());
            }
        }

        return new Batch(entries, null);
    }

    private TaxiEntry parseCsvRecord(CSVRecord record) {
        try {
            Integer taxiId = Integer.parseInt(record.get(0));
            LocalDateTime timestamp = Timestamp.parse(record.get(1));
            double latitude = Double.parseDouble(record.get(3));
            double longitude = Double.parseDouble(record.get(2));

            return new TaxiEntry(taxiId, timestamp, latitude, longitude);
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse record '" + record.toString() + "'");
            return null;
        }
    }

    private static class Batch {
        List<TaxiEntry> entries;
        TaxiEntry last;

        Batch(List<TaxiEntry> entries, TaxiEntry last) {
            this.entries = entries;
            this.last = last;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            logger.error("USAGE: <absolute-path-of-input-data>");
            return;
        }

        String filePath = args[0];

        try {
            Reader reader = new FileReader(filePath);
            DummyDataProvider provider = new DummyDataProvider();
            provider.provide(reader);
        } catch (FileNotFoundException e1) {
            logger.error("File with the given path could not be found!", e1);
        }
    }
}
