package org.apache.rocketmq.tools.command.message;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class ProducerTest {
    public static void main(String[] args) throws Exception {
        String namesrvAddr = "127.0.0.1:9876";
        String group = "test_group";
        String topic = "test_hello_rocketmq2";
        // 构建Producer实例
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setNamesrvAddr(namesrvAddr);
        producer.setProducerGroup(group);
        // 启动producer
        producer.start();
        TimeUnit.SECONDS.sleep(20);
        // 发送消息
        SendResult result = producer.send(new Message(topic,  "092912146".getBytes()), new SelectMessageQueueByHash(), "test");
        System.out.println(result.getSendStatus());
        result = producer.send(new Message(topic,  "092912147".getBytes()), new SelectMessageQueueByHash(), "test");
        System.out.println(result.getSendStatus());
        result = producer.send(new Message(topic,  "092912148".getBytes()), new SelectMessageQueueByHash(), "test");
        System.out.println(result.getSendStatus());
        result = producer.send(new Message(topic,  "092912149".getBytes()), new SelectMessageQueueByHash(), "test");
        System.out.println(result.getSendStatus());
        producer.send(new Message(topic,  "092912149".getBytes()));
        TimeUnit.SECONDS.sleep(2000);
        // 关闭producer
        producer.shutdown();
    }
}
