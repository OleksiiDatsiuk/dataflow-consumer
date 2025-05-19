package org.arpha;

public class SendEventRequest {

    private String username;
    private String password;

    public SendEventRequest(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public SendEventRequest() {
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return "SendEventRequest{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
