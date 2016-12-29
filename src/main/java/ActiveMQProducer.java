import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;
import java.util.Scanner;

/**
 * Producer class that adds messages to the ActiveMQ queue.
 */
public class ActiveMQProducer {

    private  String queueName;
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

    public static void main(String[] args) {
        ActiveMQProducer producer = new ActiveMQProducer("UpstreamQueue");
        Scanner scanner = new Scanner(System.in);
        do {
            System.out.print("Enter your name: ");
            String name = scanner.nextLine();
            System.out.print("Enter your message: ");
            String text = scanner.nextLine();
            producer.addToQueue(name, text);
            System.out.print("Message successfully sent!\n");
            System.out.print("Do you want to send another message? (y / n) ");
        } while (!scanner.nextLine().equals("n"));
        producer.close();
    }
}
