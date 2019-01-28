package com.standbyside.rabbit.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.standbyside.rabbit.consumer.common.Constants;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitConsumer {

  public static void main(String[] args) throws IOException, TimeoutException,
      InterruptedException {

    // 创建连接，和producer的略有不同
    Connection connection = createConnection();
    // 创建通道
    final Channel channel = connection.createChannel();
    // 设置客户端最多接收未被ack的消息个数
    channel.basicQos(64);

    Consumer consumer = createConsumer(channel);

    channel.basicConsume(Constants.QUEUE_NAME, consumer);

    // 等待回调函数执行完毕后，关闭资源
    TimeUnit.SECONDS.sleep(5);
    channel.close();
    connection.close();
  }

  /**
   * 创建连接.
   */
  public static Connection createConnection() throws IOException, TimeoutException {
    Address[] addresses = new Address[] {
        new Address(Constants.IP_ADDRESS, Constants.PORT)
    };
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("root");
    factory.setPassword("root1234");

    // 创建连接，和producer的略有不同
    return factory.newConnection(addresses);
  }

  /**
   * 创建消费者.
   */
  public static Consumer createConsumer(final Channel channel) {
    return new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties,
                                 byte[] body) throws IOException {
        System.out.println("receive message: " + new String(body));
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        channel.basicAck(envelope.getDeliveryTag(), false);
      }
    };
  }
}
