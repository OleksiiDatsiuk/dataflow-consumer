package org.arpha.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DataflowConsumer {
    /**
     * The topic from which messages should be consumed.
     */
    String topic();

    /**
     * The consumer group for this consumer.
     * If not specified, the default group from properties will be used.
     */
    String group() default "";

    /**
     * Initial offset for the consumer for this topic.
     * Defaults to 0.
     */
    long initialOffset() default 0L;
}
