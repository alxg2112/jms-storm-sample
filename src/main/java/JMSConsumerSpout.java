import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import javax.jms.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Spout that consumes message from ActiveMQ queue.
 */
public class JMSConsumerSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private ActiveMQConsumer jmsConsumer;
    private ActiveMQProducer jmsProducer;
    private HashMap<Object, Message> messagesToAck;
    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    public static Integer enqueuedMessages = 0;
    public static Integer dequeuedMessages = 0;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        messagesToAck = new HashMap<>();
        jmsConsumer = new ActiveMQConsumer();
        jmsProducer = new ActiveMQProducer("FailQueue");
    }

    @Override
    public void nextTuple() {
        TextMessage message = (TextMessage) jmsConsumer.getMessage();

        if (message != null) {
            try {
                Object msgId = message.hashCode();
                messagesToAck.put(msgId, message);
                enqueuedMessages++;
                collector.emit(Utils.xmlMsgToTuple(message.getText()), msgId);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name", "text"));
    }

    @Override
    public void ack(Object msgId) {
        LOGGER.info(String.format("Ack on msgId: %s", msgId));
        messagesToAck.remove(msgId);
        dequeuedMessages++;
    }

    @Override
    public void fail(Object msgId) {
        LOGGER.info(String.format("Fail on msgId: %s", msgId));
        jmsProducer.addToQueue(messagesToAck.remove(msgId));
        dequeuedMessages++;
    }

    /**
     * Consumer that gets messages from ActiveMQ queue.
     */
    private class ActiveMQConsumer {

        private ActiveMQConnectionFactory connectionFactory;
        private Connection connection;
        private Session session;
        private Destination destination;
        private MessageConsumer messageConsumer;

        private Message getMessage() {
            Message message = null;

            try {
                message = messageConsumer.receive(Utils.getProperty("jmsReceiveTimeout"));
            } catch (JMSException e) {
                LOGGER.info("Exception occurred: " + e.getMessage());
            }

            return message;
        }

        private ActiveMQConsumer() {
            try {
                connectionFactory = new ActiveMQConnectionFactory(
                        ActiveMQConnection.DEFAULT_BROKER_URL);
                connection = connectionFactory.createConnection();
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                destination = session.createQueue("UpstreamQueue");
                messageConsumer = session.createConsumer(destination);
            } catch (JMSException e) {
                LOGGER.info("Exception occurred: " + e.getMessage());
            }
        }
    }
}
