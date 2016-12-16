import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;

import javax.jms.*;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Spout that consumes message from ActiveMQ queue.
 */
public class JMSConsumerSprout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BlockingQueue<Message> pendingMessages;
    private ActiveMQConsumer jmsConsumer;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        pendingMessages = new LinkedBlockingDeque<>();
        jmsConsumer = new ActiveMQConsumer();
        jmsConsumer.run();
    }

    @Override
    public void nextTuple() {
        Message message = pendingMessages.poll();

        if (message != null) {
            try {
                collector.emit(Utils.jmsMsgToTuple(message));
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from", "text"));
    }

    private class ActiveMQConsumer implements Runnable, ExceptionListener {
        public void run() {
            try {
                ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                        ActiveMQConnection.DEFAULT_BROKER_URL);
                Connection connection = connectionFactory.createConnection();
                connection.start();
                connection.setExceptionListener(this);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Destination destination = session.createQueue("UpstreamQueue");
                MessageConsumer consumer = session.createConsumer(destination);
                consumer.setMessageListener(new ConsumerMessageListener());
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
        }

        public synchronized void onException(JMSException ex) {
            System.out.println("JMS Exception occurred. Shutting down client.");
        }

        /**
         * Listener class that listens for incoming messages in ActiveMQ queue and submits them to the queue.
         */
        private class ConsumerMessageListener implements MessageListener {
            public void onMessage(javax.jms.Message message) {
                pendingMessages.offer(message);
            }
        }
    }
}
