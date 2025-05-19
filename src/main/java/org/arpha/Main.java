package org.arpha;

public class Main {

    public static void main(String[] args) {
        GenericConsumerClient.ConsumerConfiguration<SendEventRequest> config =
                new GenericConsumerClient.ConsumerConfiguration<>(
                        "localhost",
                        53543,
                        "two-partition-topic",
                        "5two-consumer-group",
                        SendEventRequest.class,
                        msg -> System.out.println("Received: " + msg.toString()),
                        0
                );

        GenericConsumerClient<SendEventRequest> consumer = new GenericConsumerClient<>(config);
        consumer.start();
    }

}
