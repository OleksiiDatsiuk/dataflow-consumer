package org.arpha;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Message {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private MessageType type;
    private String topic;
    private String content;

    public Message(MessageType type, String topic, String content) {
        this.type = type;
        this.topic = topic;
        this.content = content;
    }

    public static Message parse(String rawMessage) {
        try {
            return OBJECT_MAPPER.readValue(rawMessage, Message.class);
        } catch (JsonMappingException e) {
            throw new IllegalArgumentException("Invalid message format: " + e.getMessage(), e);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Error processing message: " + e.getMessage(), e);
        }
    }

    public String serialize() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing message: " + e.getMessage(), e);
        }
    }

    public Message() {
    }

    public MessageType getType() {
        return type;
    }

    public void setType(MessageType type) {
        this.type = type;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}