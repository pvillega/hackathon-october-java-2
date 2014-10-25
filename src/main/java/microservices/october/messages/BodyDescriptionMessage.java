package microservices.october.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BodyDescriptionMessage {
    public String id;
    public Point location;
    @JsonProperty("vector")
    public Point velocity;
    public float mass;
}
