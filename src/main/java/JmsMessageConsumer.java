import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.jms.*;
import java.io.Serializable;
import java.util.logging.Logger;

/**
 * A JMSTupleProvider
 */
public class JmsMessageConsumer implements Serializable {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private MessageConsumer messageConsumer;
    private int receiveTimeout;

    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public Message getMessage() {
        Message message = null;

        try {
            message = messageConsumer.receive(receiveTimeout);
        } catch (JMSException e) {
            LOGGER.info("Exception occurred: " + e.getMessage());
        }

        return message;
    }

    public void close() {
        try {
            connection.close();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }

    public JmsMessageConsumer(String appContextClasspathResource, String connectionFactoryBean,
                               String upstreamDestinationBean, String jmsReceiveTimeoutBean) {
        try {
            ApplicationContext context = new ClassPathXmlApplicationContext(appContextClasspathResource);
            connectionFactory = (ConnectionFactory)context.getBean(connectionFactoryBean);
            connection = connectionFactory.createConnection();
            receiveTimeout = (Integer)context.getBean(jmsReceiveTimeoutBean);
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = (Destination)context.getBean(upstreamDestinationBean);
            messageConsumer = session.createConsumer(destination);
        } catch (JMSException e) {
            LOGGER.info("Exception occurred: " + e.getMessage());
        }
    }
}
