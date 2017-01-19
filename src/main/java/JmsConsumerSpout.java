import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Spout that consumes message from ActiveMQ queue.
 */
public class JmsConsumerSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private JmsMessageConsumer jmsConsumer;
    private JmsMessageProducer jmsProducer;
    private HashMap<Object, Message> messagesToAck;
    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        messagesToAck = new HashMap<>();
        jmsConsumer = new JmsMessageConsumer("storm-jms.xml", "jmsConnectionFactory",
                "upstreamQueue", "jmsReceiveTimeout");
        jmsProducer = new JmsMessageProducer("storm-jms.xml", "jmsConnectionFactory",
                "failQueue");
    }

    @Override
    public void nextTuple() {
        TextMessage message = (TextMessage) jmsConsumer.getMessage();

        if (message != null) {
            try {
                Object msgId = message.hashCode();
                messagesToAck.put(msgId, message);
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
    }

    @Override
    public void fail(Object msgId) {
        LOGGER.info(String.format("Fail on msgId: %s", msgId));
        jmsProducer.addToQueue(messagesToAck.remove(msgId));
    }

    @Override
    public void close() {
        jmsConsumer.close();
    }
}
