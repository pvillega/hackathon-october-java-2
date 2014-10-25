package microservices.october.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Message {
    public String who;
    public String says;

    public Message() {
    }

    public Message(String who, String says) {
        this();
        this.who = who;
        this.says = says;
    }

    @Override
    public String toString() {
        return "Message{" +
                "who='" + who + '\'' +
                ", says='" + says + '\'' +
                '}';
    }
}
