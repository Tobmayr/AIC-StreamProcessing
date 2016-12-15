package at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.averageSpeed.AverageSpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.distance.DistanceStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedStateMapper;

public class StateObjectMapperFactory {
    private final String type;

    public StateObjectMapperFactory(String type) {
        this.type = type;
    }

    public <T extends StateObject> StateObjectMapper<T> create() {
        if (type.equals("speed")) {
            return (StateObjectMapper<T>) new SpeedStateMapper();
        } else if (type.equals("avgSpeed")) {
            return (StateObjectMapper<T>) new AverageSpeedStateMapper();
        } else {
            return (StateObjectMapper<T>) new DistanceStateMapper();
        }
    }
}
