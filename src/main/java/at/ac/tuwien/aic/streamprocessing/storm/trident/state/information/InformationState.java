package at.ac.tuwien.aic.streamprocessing.storm.trident.state.information;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class InformationState implements Serializable {
    private Integer taxiCount;
    private Double distance; // overall distance
    private Set<Integer> ids;

    public InformationState(Integer taxiCount, Double distance) {
        this.taxiCount = taxiCount;
        this.distance = distance;
        ids = new HashSet<>();
    }

    public boolean addTaxi(Integer taxiId) {
        return ids.add(taxiId);
    }

    public Integer getTaxiCount() {
        return taxiCount;
    }

    public Double getDistance() {
        return distance;
    }

    @Override
    public String toString() {
        return "Information{" + "taxiCount=" + taxiCount + ", distance=" + distance + '}';
    }
}
