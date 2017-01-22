package at.ac.tuwien.aic.streamprocessing.model;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * Class representing a taxi entry.
 */
public class TaxiEntry implements Serializable {
    private int taxiId;
    private LocalDateTime timestamp;
    private double latitude;
    private double longitude;

    /**
     * Instantiates a new Taxi entry.
     *
     * @param taxiId
     *            the taxi id
     * @param timestamp
     *            the timestamp
     * @param latitude
     *            the latitude
     * @param longitude
     *            the longitude
     */
    public TaxiEntry(int taxiId, LocalDateTime timestamp, double latitude, double longitude) {
        this.taxiId = taxiId;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    /**
     * Gets taxi id.
     *
     * @return the taxi id
     */
    public int getTaxiId() {
        return taxiId;
    }

    /**
     * Gets timestamp.
     *
     * @return the timestamp
     */
    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    /**
     * Gets latitude.
     *
     * @return the latitude
     */
    public double getLatitude() {
        return latitude;
    }

    /**
     * Gets longitude.
     *
     * @return the longitude
     */
    public double getLongitude() {
        return longitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TaxiEntry{" + "taxiId=" + taxiId + ", timestamp=" + timestamp + ", latitude=" + latitude + ", longitude=" + longitude + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TaxiEntry taxiEntry = (TaxiEntry) o;

        if (taxiId != taxiEntry.taxiId)
            return false;
        if (Double.compare(taxiEntry.latitude, latitude) != 0)
            return false;
        if (Double.compare(taxiEntry.longitude, longitude) != 0)
            return false;
        return timestamp != null ? timestamp.equals(taxiEntry.timestamp) : taxiEntry.timestamp == null;

    }

    @Override
    public int hashCode() {
        int result;
        long temp;
        result = taxiId;
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        temp = Double.doubleToLongBits(latitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(longitude);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
