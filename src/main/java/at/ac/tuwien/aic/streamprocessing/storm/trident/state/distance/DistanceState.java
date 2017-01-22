package at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;

public class DistanceState implements StateObject {
    private Double latitude; // last position
    private Double longitude; // last position
    private Double distance; // distance travelled so far

    public DistanceState(Double latitude, Double longitude, Double distance) {
        this.latitude = latitude;
        this.longitude = longitude;
        this.distance = distance;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getDistance() {
        return distance;
    }

    @Override
    public String toString() {
        return "RedisDistanceState{" + "latitude=" + latitude + ", longitude=" + longitude + ", distance=" + distance + '}';
    }
}
