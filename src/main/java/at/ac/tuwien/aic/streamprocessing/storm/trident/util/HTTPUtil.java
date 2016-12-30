package at.ac.tuwien.aic.streamprocessing.storm.trident.util;

import java.io.IOException;

import org.apache.storm.shade.org.apache.http.HttpResponse;
import org.apache.storm.shade.org.apache.http.client.HttpClient;
import org.apache.storm.shade.org.apache.http.client.methods.HttpPost;
import org.apache.storm.shade.org.apache.http.entity.StringEntity;
import org.apache.storm.shade.org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HTTPUtil {
    private static final Logger logger = LoggerFactory.getLogger(HTTPUtil.class);
   

    private HTTPUtil() {
    }

    public static void sendJSONPostRequest(String uri, String data) {
        try {
            HttpClient httpClient = HttpClientBuilder.create().build();
            HttpPost post = new HttpPost(uri);
            StringEntity postingString = new StringEntity(data);
            post.setEntity(postingString);
            post.setHeader("Content-type", "application/json");
            HttpResponse response = httpClient.execute(post);
            logger.info(response.toString());
        } catch (IOException e) {
            logger.error("Caught expcetion while trying to send a post request", e);
        }

    }


}
