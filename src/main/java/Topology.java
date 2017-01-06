import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.util.logging.Logger;

/**
 * Main class used to start topology.
 */
public class Topology {

    private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    public static void main(String[] args) throws Exception {

        // Generate sample messages in ActiveMQ queue
        ActiveMQProducer.generateSampleData();

        // Build the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("jmsConsumerSpout", new JMSConsumerSpout(), 100);
        builder.setBolt("jmsProducerBolt", new JMSProducerBolt(), 100)
                .shuffleGrouping("jmsConsumerSpout");
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(10);

        // If there are arguments, we are running on cluster, otherwise locally
        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("jmsProxy", conf, builder.createTopology());
            Thread.sleep(Utils.getProperty("clusterShutdownTimeout"));
            cluster.shutdown();
        }

        LOGGER.info("Enqueued messages: " + JMSConsumerSpout.enqueuedMessages);
        LOGGER.info("Dequeued messages: " + JMSConsumerSpout.dequeuedMessages);
        System.exit(0);
    }
}
