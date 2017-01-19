import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.*;
import java.io.Serializable;
import java.util.logging.Logger;

/**
 * Producer class that adds messages to the ActiveMQ queue.
 */
public class JmsMessageProducer implements Serializable {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageProducer producer;

    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public JmsMessageProducer(String appContextClasspathResource, String connectionFactoryBean,
                              String downstreamDestinationBean) {
        try {
            ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
            connectionFactory = (ConnectionFactory)context.getBean(connectionFactoryBean);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = (Destination)context.getBean(downstreamDestinationBean);
            producer = session.createProducer(destination);
        } catch (JMSException e) {
            LOGGER.info("Exception occurred: " + e.getMessage());
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
        JmsMessageProducer producer = new JmsMessageProducer("storm-jms.xml",
                "jmsConnectionFactory", "upstreamQueue");

        for (int i = 0; i < 1000; i++) {
            producer.addToQueue("test" + i, "test" + (1000 - i));
        }

        producer.close();
    }
}
