package at.ac.tuwien.aic.streamprocessing.storm.trident.util;

import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HttpUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpUtil.class);


    private HttpUtil() {
    }

    public static void sendJSONPostRequest(String uri, String data) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(uri);
            StringEntity postingString = new StringEntity(data);
            post.setEntity(postingString);
            post.setHeader("Content-type", "application/json");
            HttpResponse response = httpClient.execute(post);
        } catch (IOException e) {
            logger.error("Caught expcetion while trying to send a post request", e);
        }

    }

    public static String toJSON(Map<String, String> map) {
        String response = "{";
        for (String key : map.keySet()) {
            response += String.format("\"%s\":\"%s\",", key, map.get(key));
        }
        return response.substring(0, response.length() - 1) + "}";

    }


}
