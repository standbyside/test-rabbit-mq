package com.standbyside.rabbit.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.standbyside.rabbit.producer.common.Constants;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitProducer {

  public static void main(String[] args) throws IOException, TimeoutException {

    // 创建连接
    Connection connection = createConnection();
    // 创建信道
    Channel channel = connection.createChannel();
    // 创建一个type=direct、持久化、非自动删除的exchange
    channel.exchangeDeclare(Constants.EXCHANGE_NAME, "direct", true, false, null);
    // 创建一个持久化、非排他的、非自动删除的queue
    channel.queueDeclare(Constants.QUEUE_NAME, true, false, false, null);
    // 将exchange与queue通过routingKey绑定
    channel.queueBind(Constants.QUEUE_NAME, Constants.EXCHANGE_NAME, Constants.ROUTING_KEY);

    // 发送消息
    String message = "Hello World！";
    channel.basicPublish(Constants.EXCHANGE_NAME, Constants.ROUTING_KEY,
        MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

    // 关闭资源
    channel.close();
    connection.close();
  }

  /**
   * 创建连接.
   */
  public static Connection createConnection() throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(Constants.IP_ADDRESS);
    factory.setPort(Constants.PORT);
    factory.setUsername("root");
    factory.setPassword("root1234");
    return factory.newConnection();
  }
}
