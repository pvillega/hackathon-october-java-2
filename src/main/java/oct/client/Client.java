package oct.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class Client {
    final ObjectMapper objectMapper = new ObjectMapper();
    final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final String serviceUrl;

    public Client(String serviceUrl) {
        this.serviceUrl = serviceUrl;
    }

    public void send(String topic, Object message) throws Exception {
        System.out.println("Sending " + message);
        HttpPost httpPost = new HttpPost(serviceUrl + "/topics/" + topic + "/facts");
        String payload = objectMapper.writeValueAsString(message);
        httpPost.setEntity(new StringEntity(payload));
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            EntityUtils.consume(response.getEntity());
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_ACCEPTED) {
                throw new RuntimeException("Submission failed: " + response.getStatusLine());
            }
        }
    }

    public Subscription subscribe(String topic) throws Exception {
        HttpPost httpPost = new HttpPost(serviceUrl + "/topics/" + topic + "/subscriptions");
        try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
            String responsePayload = EntityUtils.toString(response.getEntity());
            SubscriptionResponse sr = objectMapper.readValue(responsePayload, SubscriptionResponse.class);
            return new Subscription(this, sr.retrieval_url);
        }
    }
}
