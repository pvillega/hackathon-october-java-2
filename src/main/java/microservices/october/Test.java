package microservices.october;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import microservices.october.messages.*;

import java.io.IOException;

public class Test {
    public static final String EXCHANGE_NAME = "combo";
    public static final String CONNECTION_HOST = "54.77.246.25";
    public static final int CONNECTION_PORT = 5672;

    private ConnectionFactory connectionFactory = setUpConnectionFactory();
    private Publisher publisher = new Publisher(connectionFactory, EXCHANGE_NAME);
    private Subscriber subscriber = new Subscriber(connectionFactory, EXCHANGE_NAME, "chat");
    private ObjectMapper objectMapper = new ObjectMapper();
    private String lastPerson = "";

    private ConnectionFactory setUpConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONNECTION_HOST);
        factory.setPort(CONNECTION_PORT);
        return factory;
    }

    private void processOneMessage() throws IOException {
        QueueingConsumer.Delivery delivery = subscriber.getNextMessage();
        String message = new String(delivery.getBody());
        String topic = delivery.getEnvelope().getRoutingKey();

        System.out.println("Received a message on '" + topic + "' '" + message + "'");

        if (topic.equals("chat")) {
            Message m = objectMapper.readValue(message, Message.class);
            if (m.who == null) {
                return;
            }
            if (m.who.equals("Szymon") || m.who.equals(lastPerson)) {
                return;
            }
            lastPerson = m.who;

            Message r = new Message("Szymon", "Hello " + m.who + "!");
            publisher.publish("chat", objectMapper.writeValueAsString(r));
        }
    }

    public void enterInfiniteLoop() throws Exception {
        Message m = new Message("Szymon", "Hello!");
        publisher.publish("chat", objectMapper.writeValueAsString(m));
        while (true) {
            try {
                processOneMessage();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Test().enterInfiniteLoop();
    }
}
