import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import javax.jms.*;
import java.util.Map;

/**
 * Bolt that submits message to ActiveMQ queue.
 */
public class JMSProducerBolt extends BaseBasicBolt {

    private ActiveMQProducer producer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        producer = new ActiveMQProducer();
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String from = tuple.getStringByField("from");
        String text = tuple.getStringByField("text");

        if (text.equals("")) {
            throw new IllegalArgumentException();
        }

        producer.addToQueue(from, text);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from", "text"));
    }

    /**
     * Producer class that adds messages to the ActiveMQ queue.
     */
    private static class ActiveMQProducer {

        private static final String QUEUE_NAME = "DownstreamQueue";
        private ConnectionFactory connectionFactory;
        private Connection connection;
        private Session session;
        private Destination destination;
        private MessageProducer producer;

        private ActiveMQProducer() {
            connectionFactory = new ActiveMQConnectionFactory(ActiveMQConnection.DEFAULT_BROKER_URL);
            configure();
        }

        private void configure() {
            try {
                connection = connectionFactory.createConnection();
                connection.start();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                destination = session.createQueue(QUEUE_NAME);
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }

        public void addToQueue(String from, String text) {
            try {
                producer = session.createProducer(destination);
                MapMessage message = session.createMapMessage();
                message.setString("from", from);
                message.setString("text", text);
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
    }
}
