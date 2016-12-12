package at.ac.tuwien.aic.streamprocessing.cli;

import redis.clients.jedis.Jedis;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class RedisMonitor {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int INTERVAL = 500;

    private final Jedis jedis;

    static {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.CEILING);
    }

    RedisMonitor() {
        jedis = new Jedis(REDIS_HOST, REDIS_PORT);
    }

    void run() {
        while (true) {
            List<TaxiData> taxis = getTaxiIds().stream()
                    .map(this::queryTaxi)
                    .collect(Collectors.toList());
            printData(taxis);

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {

            }
        }
    }

    private List<Integer> getTaxiIds() {
        Set<Integer> ids = new HashSet<>();

        for (String key : jedis.keys("*")) {
            if (!key.contains("_dist") && !key.contains("_avgSpeed")) {
                continue;
            }

            Integer id = Integer.parseInt(key.split("_")[0]);
            ids.add(id);
        }

        List<Integer> sortedIds = new ArrayList<>(ids);
        Collections.sort(sortedIds);

        return sortedIds;
    }

    private TaxiData queryTaxi(Integer id) {
        String distance = jedis.get(id.toString() + "_dist");
        String averageSpeed = jedis.get(id.toString() + "_avgSpeed");

        return new TaxiData(
                id.toString(),
                round(distance),
                round(averageSpeed)
        );
    }

    private void printData(List<TaxiData> taxis) {
        System.out.println("\033[H\033[2J");
        System.out.flush();
        System.out.println("Taxi ID\t\tDistance (km)\tAverage Speed (km/h)");
        System.out.println("----------------------------------------------------");
        for (TaxiData taxi : taxis) {
            System.out.println(taxi.id + "\t\t" + taxi.distance + "\t\t" + taxi.averageSpeed);
        }
    }

    private String round(String value) {
        if (value == null) {
            return "N/A";
        }

        Double val = Double.parseDouble(value);
        return DECIMAL_FORMAT.format(val);
    }

    public static void main(String[] args) {
        RedisMonitor monitor = new RedisMonitor();
        monitor.run();
    }

    private static class TaxiData {
        String id;
        String distance;
        String averageSpeed;

        TaxiData(String id, String distance, String averageSpeed) {
            this.id = id;
            this.distance = distance;
            this.averageSpeed = averageSpeed;
        }
    }
}
