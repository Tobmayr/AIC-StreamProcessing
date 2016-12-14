package at.ac.tuwien.aic.streamprocessing.storm.trident.averageSpeed;

/**
 * Storage class
 *
 */

public class AvgSpeed {
    public String lastTimestamp;
    public Double avgSpeed;
    public Double hours; // time driving

    public String toRedisValue() {
        return lastTimestamp + "," + avgSpeed + "," + hours;
    }

    public static AvgSpeed fromRedisValue(String input) {
        String parts[] = input.split(",");
        AvgSpeed speed = new AvgSpeed();
        speed.lastTimestamp = parts[0];
        speed.avgSpeed = Double.parseDouble(parts[1]);
        speed.hours = Double.parseDouble(parts[2]);
        return speed;
    }

}
