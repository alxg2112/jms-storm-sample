import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * Producer class that adds messages to the ActiveMQ queue.
 */
public class ActiveMQProducer {

    private String queueName;
    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;

    public ActiveMQProducer(String queueName) {
        connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
        this.queueName = queueName;
        configure();
    }

    private void configure() {
        try {
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue(queueName);
            producer = session.createProducer(destination);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void addToQueue(String name, String text) {
        try {
            TextMessage message = session.createTextMessage();
            message.setText(Utils.generateXmlMessage(name, text));
            producer.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void addToQueue(Message message) {
        try {
            producer.send(message);
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public static void generateSampleData() {
        ActiveMQProducer producer = new ActiveMQProducer("UpstreamQueue");

        for (int i = 0; i < 1000; i++) {
            producer.addToQueue("test" + i, "test" + (1000 - i));
            producer.addToQueue("test" + (i + 1000), "");
        }

        producer.close();
    }
}
