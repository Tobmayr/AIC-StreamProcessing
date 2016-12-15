package at.ac.tuwien.aic.streamprocessing.storm.trident.persist;

public enum InfoType {
    AVERAGE_SPEED("_avgSpeed", "avgSpeed"), DISTANCE("_dist", "distance");

    private final String fieldName;
    private final String keyPrefix;

    InfoType(String keyPrefix, String fieldName) {
        this.keyPrefix = keyPrefix;
        this.fieldName = fieldName;
    }

    public String getKeyPrefix() {
        return keyPrefix;
    }

    public String getFieldName() {
        return fieldName;
    }
}
