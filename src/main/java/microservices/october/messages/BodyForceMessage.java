package microservices.october.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BodyForceMessage {
    public String id;
    public Point force;
}
