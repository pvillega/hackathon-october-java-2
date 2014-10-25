package microservices.october;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import microservices.october.messages.BodyCreationMessage;
import microservices.october.messages.BodyDestroyedMessage;
import microservices.october.messages.BodyMovementMessage;
import microservices.october.messages.TimeMessage;
import microservices.october.messages.UniverseMessage;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static final String EXCHANGE_NAME = "combo";
    //public static final String CONNECTION_HOST = "54.77.246.25";
    public static final String CONNECTION_HOST = "178.62.106.39";
    public static final int CONNECTION_PORT = 5672;
    public static final String BODY_CREATED_TOPIC = "body.created";
    public static final String BODY_DESTROYED_TOPIC = "body.destroyed";
    public static final String TIME_TOPIC = "time";
    public static final String BODY_MOVEMENT_TOPIC = "body.movement";
    public static final String UNIVERSE_TOPIC = "universe";

    private ConnectionFactory connectionFactory = setUpConnectionFactory();
    private Publisher publisher = new Publisher(connectionFactory, EXCHANGE_NAME);
    private Subscriber subscriber = new Subscriber(connectionFactory, EXCHANGE_NAME, TIME_TOPIC, BODY_CREATED_TOPIC, BODY_DESTROYED_TOPIC, BODY_MOVEMENT_TOPIC);
    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, BodyCreationMessage> bodies = new HashMap<>();

    private ConnectionFactory setUpConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONNECTION_HOST);
        factory.setPort(CONNECTION_PORT);
        return factory;
    }

    private void handleBodyCreation(BodyCreationMessage bodyCreationMessage) throws Exception {
        bodies.put(bodyCreationMessage.id, bodyCreationMessage);
    }

    private void handleBodyDestroyed(BodyDestroyedMessage bodyDestroyedMessage) {
        bodies.remove(bodyDestroyedMessage.id);
    }

    private void handleBodyMovement(BodyMovementMessage bodyMovementMessage) {
        BodyCreationMessage bodyCreationMessage = bodies.get(bodyMovementMessage.id);
        if (bodyCreationMessage == null) {
            System.out.println("Received invalid id from bodyMovement" + bodyMovementMessage.id);
            return;
        }

        bodyCreationMessage.location = bodyMovementMessage.location;
        bodyCreationMessage.velocity = bodyMovementMessage.velocity;
    }

    private void handleTimeMessage(TimeMessage timeMessage) throws Exception {
        UniverseMessage universeMessage = new UniverseMessage();
        universeMessage.time = timeMessage.time;
        universeMessage.bodies = bodies.values();
        publisher.publish(UNIVERSE_TOPIC, objectMapper.writeValueAsString(universeMessage));
    }

    private void processOneMessage() throws Exception {
        QueueingConsumer.Delivery delivery = subscriber.getNextMessage();
        String message = new String(delivery.getBody());
        String topic = delivery.getEnvelope().getRoutingKey();

        System.out.println("Received a message on '" + topic + "' '" + message + "'");

        if (topic.equals(BODY_CREATED_TOPIC)) {
            BodyCreationMessage bodyCreationMessage = objectMapper.readValue(message, BodyCreationMessage.class);
            handleBodyCreation(bodyCreationMessage);
        } else if (topic.equals(BODY_MOVEMENT_TOPIC)) {
            BodyMovementMessage bodyMovementMessage = objectMapper.readValue(message, BodyMovementMessage.class);
            handleBodyMovement(bodyMovementMessage);
        } else if (topic.equals(TIME_TOPIC)) {
            TimeMessage timeMessage = objectMapper.readValue(message, TimeMessage.class);
            handleTimeMessage(timeMessage);
        } else if (topic.equals(BODY_DESTROYED_TOPIC)) {
            BodyDestroyedMessage bodyDestroyedMessage = objectMapper.readValue(message, BodyDestroyedMessage.class);
            handleBodyDestroyed(bodyDestroyedMessage);
        }
    }

    public void enterInfiniteLoop() throws Exception {
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
