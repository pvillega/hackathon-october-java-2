package microservices.october;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import microservices.october.messages.BodyDescriptionMessage;
import microservices.october.messages.BodyForceMessage;
import microservices.october.messages.BodyMovementMessage;
import microservices.october.messages.Point;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static final String EXCHANGE_NAME = "combo";
    //public static final String CONNECTION_HOST = "54.77.246.25";
    public static final String CONNECTION_HOST = "178.62.106.39";
    public static final int CONNECTION_PORT = 5672;
    public static final String BODY_FORCE_TOPIC = "body.force";
    public static final String BODY_DESCRIPTION_TOPIC = "body.created";
    public static final String BODY_MOVEMENT_TOPIC = "body.movement";

    private ConnectionFactory connectionFactory = setUpConnectionFactory();
    private Publisher publisher = new Publisher(connectionFactory, EXCHANGE_NAME);
    private Subscriber subscriber = new Subscriber(connectionFactory, EXCHANGE_NAME, BODY_FORCE_TOPIC, BODY_DESCRIPTION_TOPIC);
    private ObjectMapper objectMapper = new ObjectMapper();
    private Map<String, BodyDescriptionMessage> bodies = new HashMap<>();

    private ConnectionFactory setUpConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(CONNECTION_HOST);
        factory.setPort(CONNECTION_PORT);
        return factory;
    }
//         Message m = new Message("Szymon", "Hello! (RabbitMQ)");
//    publisher.publish("chat", objectMapper.writeValueAsString(m));

    private void handleBodyDescription(BodyDescriptionMessage bodyDescriptionMessage) throws Exception {
//        BodyMovementMessage bodyMovementMessage = new BodyMovementMessage();
//        bodyMovementMessage.id = bodyDescriptionMessage.id;
//        bodyMovementMessage.location = bodyDescriptionMessage.location;
//        bodyMovementMessage.velocity = bodyDescriptionMessage.velocity;
//        publisher.publish(BODY_MOVEMENT_TOPIC, objectMapper.writeValueAsString(bodyMovementMessage));
        bodies.put(bodyDescriptionMessage.id, bodyDescriptionMessage);
    }

    private void handleBodyForce(BodyForceMessage bodyForceMessage) throws Exception {
        // 1. check if there is a body with provided id
        BodyDescriptionMessage bodyDescription = bodies.get(bodyForceMessage.id);
        if (bodyDescription == null) {
            System.out.println("Unknown body: " + bodyForceMessage.id);
            return;
        }
        // 2. get mass of it
        float mass = bodyDescription.mass;
        // 3. calculate new velocity
        float ax = bodyForceMessage.force.x / mass;
        float ay = bodyForceMessage.force.y / mass;

        bodyDescription.velocity.x += ax*1;
        bodyDescription.velocity.y += ay*1;
        // 4. calculate new location
        bodyDescription.location.x += bodyDescription.velocity.x;
        bodyDescription.location.y += bodyDescription.velocity.y;
        // 5. publish

        BodyMovementMessage bodyMovementMessage = new BodyMovementMessage();
        bodyMovementMessage.id = bodyDescription.id;
        bodyMovementMessage.location = bodyDescription.location;
        bodyMovementMessage.velocity = bodyDescription.velocity;
        publisher.publish(BODY_MOVEMENT_TOPIC, objectMapper.writeValueAsString(bodyMovementMessage));
    }

    private void processOneMessage() throws Exception {
        QueueingConsumer.Delivery delivery = subscriber.getNextMessage();
        String message = new String(delivery.getBody());
        String topic = delivery.getEnvelope().getRoutingKey();

        System.out.println("Received a message on '" + topic + "' '" + message + "'");

        if (topic.equals(BODY_DESCRIPTION_TOPIC)) {
            BodyDescriptionMessage bodyDescriptionMessage = objectMapper.readValue(message, BodyDescriptionMessage.class);
            handleBodyDescription(bodyDescriptionMessage);
        } else if (topic.equals(BODY_FORCE_TOPIC)) {
            BodyForceMessage bodyForceMessage = objectMapper.readValue(message, BodyForceMessage.class);
            handleBodyForce(bodyForceMessage);
        }
    }

    public void enterInfiniteLoop() throws Exception {
//        BodyMovementMessage bodyMovementMessage = new BodyMovementMessage();
//        bodyMovementMessage.id = "123.111.334";
//        bodyMovementMessage.location = new Point(1.2f, 2.3f);
//        bodyMovementMessage.velocity = new Point(2.5f, -2.9f);
//        publisher.publish(BODY_MOVEMENT_TOPIC, objectMapper.writeValueAsString(bodyMovementMessage));

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
