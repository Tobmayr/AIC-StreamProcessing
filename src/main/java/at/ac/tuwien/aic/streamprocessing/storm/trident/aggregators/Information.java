package at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators;

import java.io.Serializable;

public class Information implements Serializable{
	private Integer taxiCount;
	private Double overallDistance;

	public Information(Integer taxiCount, Double overallDistance) {
		super();
		this.taxiCount = taxiCount;
		this.overallDistance = overallDistance;
	}

	public Integer getTaxiCount() {
		return taxiCount;
	}

	public Double getOverallDistance() {
		return overallDistance;
	}

	public Information add(Information information) {
		return new Information(this.taxiCount + information.getTaxiCount(),
				this.overallDistance + information.getOverallDistance());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((overallDistance == null) ? 0 : overallDistance.hashCode());
		result = prime * result + ((taxiCount == null) ? 0 : taxiCount.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Information other = (Information) obj;
		if (overallDistance == null) {
			if (other.overallDistance != null)
				return false;
		} else if (!overallDistance.equals(other.overallDistance))
			return false;
		if (taxiCount == null) {
			if (other.taxiCount != null)
				return false;
		} else if (!taxiCount.equals(other.taxiCount))
			return false;
		return true;
	}
	
	

}
