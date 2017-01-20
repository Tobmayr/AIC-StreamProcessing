package at.ac.tuwien.aic.streamprocessing.storm.trident.dashboard;

import java.io.IOException;
import java.util.Map;

import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClientBuilder;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DashboardNotifier extends BaseFilter {
    private final Logger logger = LoggerFactory.getLogger(DashboardNotifier.class);
    private String dashboardURI;

    public DashboardNotifier(String dashboardURI) {
        this.dashboardURI = dashboardURI;
    }
 
    protected void sendJSONPostRequest(Map<String, String> parameters) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(dashboardURI);
            String data = toJSON(parameters);
            StringEntity postingString = new StringEntity(data);
            post.setEntity(postingString);
            post.setHeader("Content-type", "application/json");
            httpClient.execute(post);
        } catch (IOException e) {
            logger.error("Caught expcetion while trying to send a post request", e);
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
