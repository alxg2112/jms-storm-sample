import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Bolt that submits message to ActiveMQ queue.
 */
public class JMSProducerBolt extends BaseRichBolt {

    private ActiveMQProducer jmsProducer;
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("name", "text"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        jmsProducer = new ActiveMQProducer("DownstreamQueue");
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            processTuple(tuple);
        } catch (FailedException e) {
            collector.fail(tuple);
        }
    }

    /**
     * Validate tuple and send message to ActiveMQ queue if it's ok.
     *
     * @param tuple tuple to be processed.
     */
    private void processTuple(Tuple tuple) throws FailedException {
        String name = tuple.getStringByField("name");
        String text = tuple.getStringByField("text");

        if (text.equals("") || name.equals("")) {
            throw new FailedException("Message has empty text field!");
        } else {
            jmsProducer.addToQueue(name, text);
            collector.ack(tuple);
        }
    }
}
