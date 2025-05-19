package org.arpha.configuration;

import org.arpha.GenericConsumerClient;
import org.arpha.annotation.DataflowConsumer;
import org.arpha.properties.DataflowConsumerProperties;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Configuration
@EnableConfigurationProperties(DataflowConsumerProperties.class)
public class DataflowConsumerAutoConfiguration implements BeanPostProcessor, ApplicationContextAware, DisposableBean {

    private ApplicationContext applicationContext;
    private final DataflowConsumerProperties properties;
    private final Map<String, List<GenericConsumerClient<?>>> consumerClients = new ConcurrentHashMap<>();

    public DataflowConsumerAutoConfiguration(DataflowConsumerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        Map<Method, DataflowConsumer> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                (MethodIntrospector.MetadataLookup<DataflowConsumer>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, DataflowConsumer.class));

        if (!annotatedMethods.isEmpty()) {
            List<GenericConsumerClient<?>> clientsForBean = new ArrayList<>();
            annotatedMethods.forEach((method, annotation) -> {
                if (method.getParameterCount() != 1) {
                    System.out.println("Method %s annotated with @DataflowConsumer must have exactly one parameter.".formatted(method.getName()));
                    return;
                }
                Class<?> messageType = method.getParameterTypes()[0];
                clientsForBean.add(registerConsumer(bean, method, annotation, messageType));
            });
            if (!clientsForBean.isEmpty()) {
                this.consumerClients.put(beanName, clientsForBean);
            }
        }
        return bean;
    }

    private <T> GenericConsumerClient<T> registerConsumer(Object bean, Method method, DataflowConsumer annotation, Class<T> messageType) {
        String topic = annotation.topic();
        String group = annotation.group().isEmpty() ? properties.getDefaultConsumerGroup() : annotation.group();
        long initialOffset = annotation.initialOffset();

        System.out.println("Registering Dataflow consumer for topic [%s], group [%s], method [%s.%s] with message type [%s]".formatted(
                topic, group, bean.getClass().getSimpleName(), method.getName(), messageType.getSimpleName()));

        Consumer<T> handler = message -> {
            try {
                ReflectionUtils.makeAccessible(method);
                method.invoke(bean, message);
            } catch (IllegalAccessException | InvocationTargetException e) {
                System.out.println("Error processing message in @DataflowConsumer method %s.%s: %s".formatted(
                        bean.getClass().getSimpleName(), method.getName(), e.getCause() != null ? e.getCause() : e));
            }
        };

        GenericConsumerClient.ConsumerConfiguration<T> config =
                new GenericConsumerClient.ConsumerConfiguration<>(
                        properties.getClusterHost(),
                        properties.getClusterPort(),
                        topic,
                        group,
                        messageType,
                        handler,
                        initialOffset
                );

        GenericConsumerClient<T> consumerClient = new GenericConsumerClient<>(config);
        consumerClient.start();
        return consumerClient;
    }

    @Override
    public void destroy() {
        System.out.println("Shutting down all Dataflow consumer clients...");
        consumerClients.values().forEach(clients -> clients.forEach(GenericConsumerClient::stop));
        consumerClients.clear();
        System.out.println("All Dataflow consumer clients stopped.");
    }
}
