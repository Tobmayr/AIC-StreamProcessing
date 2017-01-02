package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

import at.ac.tuwien.aic.streamprocessing.storm.trident.state.information.InformationState;

public class CalculateTaxiCountAndDistance implements CombinerAggregator<InformationState> {

    @Override
    public InformationState init(TridentTuple tuple) {
        return new InformationState(1, tuple.getDoubleByField("distance"));
    }

    @Override
    public InformationState combine(InformationState i1, InformationState i2) {
        return new InformationState(i1.getTaxiCount() + i2.getTaxiCount(), i1.getDistance() + i2.getDistance());
    }

    @Override
    public InformationState zero() {
        return new InformationState(0, 0D);
    }
}
