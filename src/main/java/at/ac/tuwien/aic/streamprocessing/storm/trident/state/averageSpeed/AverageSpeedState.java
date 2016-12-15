package at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObject;

public class AverageSpeedState implements StateObject {
    private final Integer observations;
    private final Double speedSum;

    public AverageSpeedState(Integer observations, Double speedSum) {
        this.observations = observations;
        this.speedSum = speedSum;
    }

    public Integer getObservations() {
        return observations;
    }

    public Double getSpeedSum() {
        return speedSum;
    }

    @Override
    public String toString() {
        return "AverageSpeedState{" +
                "observations=" + observations +
                ", speedSum=" + speedSum +
                '}';
    }
}
