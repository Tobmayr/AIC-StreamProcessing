package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard.optimization;


import at.ac.tuwien.aic.streamprocessing.model.utils.Timestamp;
import at.ac.tuwien.aic.streamprocessing.storm.trident.aggregators.Aggregator;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.objects.StateObjectMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedState;
import at.ac.tuwien.aic.streamprocessing.storm.trident.state.speed.SpeedStateMapper;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Constants;
import at.ac.tuwien.aic.streamprocessing.storm.trident.util.Haversine;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class OptimizedCalculateSpeedAndSpeedingNotifier extends Aggregator<SpeedState> {

    private final Logger logger = LoggerFactory.getLogger(OptimizedCalculateSpeedAndSpeedingNotifier.class);

    private String dashboardAddress;

    private StateObjectMapper<SpeedState> mapper;

    public OptimizedCalculateSpeedAndSpeedingNotifier(String dashboardAddress) {
        this.dashboardAddress = dashboardAddress;
    }


    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        this.mapper = new SpeedStateMapper();
    }


    @Override
    protected SpeedState compute(SpeedState previous, TridentTuple tuple) {
        Integer taxiId = tuple.getIntegerByField("id");
        String timestamp = tuple.getStringByField("timestamp");
        Double currentLatitude = tuple.getDoubleByField("latitude");
        Double currentLongitude = tuple.getDoubleByField("longitude");

        Double distance = Haversine.calculateDistanceBetween(
                previous.getLatitude(), previous.getLongitude(),
                currentLatitude, currentLongitude);

        LocalDateTime startTime = Timestamp.parse(previous.getTimestamp());
        LocalDateTime endTime = Timestamp.parse(timestamp);

        Double time = ChronoUnit.MILLIS.between(startTime, endTime) / (60. * 60.0 * 1000.0);

        Double speed;
        if (Double.compare(time, 0.0) == 0) {
            speed = 0.0;
        } else {
            speed = distance / time;  // in kmh
        }

        //Notify dashboard once if taxi is speeding over the limit
        if (speed >= Constants.SPEED_LIMIT) {
            Map<String, String> map = new HashMap<>();
            map.put("taxiId", Integer.toString(taxiId));
            map.put("speed", Double.toString(speed));
            sendJSONPostRequest(map, dashboardAddress + Constants.NOTIFY_SPEEDING_INCIDENT_URI);
        }

        logger.debug(
                "(speed): [taxiId={}, timestamp={}, latitude={}, longitude={}, speed={}]",
                taxiId,
                timestamp,
                currentLatitude,
                currentLongitude,
                String.format("%.3f", speed)
        );

        return new SpeedState(timestamp, currentLatitude, currentLongitude, speed);
    }

    @Override
    protected StateObjectMapper<SpeedState> getMapper() {
        return mapper;
    }

    private void sendJSONPostRequest(Map<String, String> parameters, String dashboardURI) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(dashboardURI);
            String data = toJSON(parameters);
            StringEntity postingString = new StringEntity(data);
            post.setEntity(postingString);
            post.setHeader("Content-type", "application/json");
            httpClient.execute(post);
        } catch (IOException e) {
            logger.error("Caught exception while trying to send a post request", e);
        }

    }

    private String toJSON(Map<String, String> map) {
        if (map == null) {
            return "{}";
        }
        String response = "{";
        for (String key : map.keySet()) {
            response += String.format("\"%s\":\"%s\",", key, map.get(key));
        }
        return response.substring(0, response.length() - 1) + "}";

    }
}
