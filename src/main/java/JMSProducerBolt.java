import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
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
        String name = tuple.getStringByField("name");
        String text = tuple.getStringByField("text");
        jmsProducer.addToQueue(name, text);
        collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        jmsProducer.close();
    }
}
