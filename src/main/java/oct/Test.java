package oct;

import microservices.october.messages.Message;
import oct.client.Client;
import oct.client.Subscription;

public class Test {


    private String lastPerson = "";

    public static void main(String[] args) throws Exception {
        Message m = new Message();
        m.who = "Szymon";
        m.says = "Hello!";

        //Client client = new Client("http://combo-squirrel.herokuapp.com");
        Client client = new Client("http://178.62.106.39");
        Subscription chatSub = client.subscribe("chat");
        client.send("chat", m);

        for (; ; ) {
            Message message;
            try {
                message = chatSub.readMessage(Message.class);
                if (message == null) {
                    System.out.println("Received nothing");
                    Thread.sleep(100);
                    continue;
                }
                System.out.println(message.who + ": " + message.says);
                if (!"Szymon".equals(message.who)) {
                    Message resp = new Message();
                    resp.who = "Szymon";
                    resp.says = "Hello " + message.who + "! (HTTP)";
                    client.send("chat", resp);
                }
            } catch (Exception e) {
                System.out.println("Error reading a message: " + e.getMessage());
            }
        }
    }
}
