package at.ac.tuwien.aic.streamprocessing.storm.trident.speed;

/**
 * Storage class
 *
 */

public class Position {
    public String timestamp;
    public Double latitude;
    public Double longitude;

    @Override
    public String toString() {
        return "Position{" +
                "timestamp='" + timestamp + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }

    public String toRedisValue() {
        return timestamp + ',' + latitude + "," + longitude;
    }

    public static Position fromRedisValue(String input) {
        String parts[] = input.split(",");
        Position position = new Position();
        position.timestamp = parts[0];
        position.latitude = Double.parseDouble(parts[1]);
        position.longitude = Double.parseDouble(parts[2]);
        return position;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Double getLongitude() {
        return longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }
}
