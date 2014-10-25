package oct.client;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

public class Subscription {
    private final Client client;
    private final String url;

    public Subscription(Client client, String url) {
        this.client = client;
        this.url = url;
        System.out.println("New subscription: " + url);
    }

    public <T> T readMessage(Class<T> clazz) throws Exception {
        HttpGet httpGet = new HttpGet(url);
        try (CloseableHttpResponse response = client.httpClient.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_NO_CONTENT) {
                return null;
            } else if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String payload = EntityUtils.toString(response.getEntity());
                System.out.println("Received " + payload);
                return client.objectMapper.readValue(payload, clazz);
            } else {
                throw new RuntimeException("Subscription failed: " + response.getStatusLine());
            }
        }
    }
}
