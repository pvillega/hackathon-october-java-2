package microservices.october.messages;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BodyForceMessage2 {
    public String id;
    public float[] force;
}
