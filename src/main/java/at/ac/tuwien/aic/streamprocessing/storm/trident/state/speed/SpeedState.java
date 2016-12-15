package at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;

public class SpeedState implements StateObject {

    private final String timestamp;
    private final Double latitude;
    private final Double longitude;
    private final Double speed;

    public SpeedState(String timestamp, Double latitude, Double longitude, Double speed) {
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.speed = speed;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getSpeed() {
        return speed;
    }

    @Override
    public String toString() {
        return "SpeedState{" +
                "timestamp='" + timestamp + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", speed=" + speed +
                '}';
    }
}
