import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Main class used to start topology.
 */
public class Topology {

    public static void main(String[] args) throws Exception {

        // Generate sample messages in ActiveMQ queue
        JmsMessageProducer.generateSampleData();

        // Get configuration properties
        ApplicationContext context = new ClassPathXmlApplicationContext("storm-jms.xml");
        int jmsConsumerSpoutParallelism = (Integer)context.getBean("jmsConsumerSpoutParallelism");
        int jmsProducerBoltParallelism = (Integer)context.getBean("jmsProducerBoltParallelism");
        int clusterNumWorkers = (Integer)context.getBean("clusterNumWorkers");
        int clusterShutdownTimeout = (Integer)context.getBean("clusterShutdownTimeout");

        // JMS consumer spout
        JmsConsumerSpout jmsConsumerSpout = new JmsConsumerSpout();

        // JMS producer bolt
        JmsProducerBolt jmsProducerBolt = new JmsProducerBolt();

        // Build the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("jmsConsumerSpout", jmsConsumerSpout,
                jmsConsumerSpoutParallelism);
        builder.setBolt("jmsProducerBolt", jmsProducerBolt,
                jmsProducerBoltParallelism)
                .shuffleGrouping("jmsConsumerSpout");
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(clusterNumWorkers);

        // If there are arguments, we are running on cluster, otherwise locally
        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-jms", conf, builder.createTopology());
            Thread.sleep(clusterShutdownTimeout);
            cluster.shutdown();
        }
        
        System.exit(0);
    }
}
