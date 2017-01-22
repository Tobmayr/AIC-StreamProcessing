package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class CountAndDistanceAggregator implements CombinerAggregator<Information> {
	private Map<Integer, Double> idDistMap = Collections.synchronizedMap(new HashMap<Integer, Double>());

	@Override
	public Information init(TridentTuple tuple) {
		Integer id = tuple.getIntegerByField("id");
		Double distance = tuple.getDoubleByField("distance");
		Double prevDist = idDistMap.put(id, distance);
		if (prevDist == null) {
			return new Information(1, distance);
		} else {
			return new Information(0, distance - prevDist);
		}
	}

	@Override
	public Information combine(Information val1, Information val2) {
		return val1.add(val2);
	}

	@Override
	public Information zero() {
		return new Information(0, 0D);
	}

}
