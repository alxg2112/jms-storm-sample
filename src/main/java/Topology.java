import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Main class used to start topology.
 */
public class Topology {
    public static void main(String[] args) throws Exception {
        // Build the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("jmsConsumerSpout", new JMSConsumerSprout(), 5);
        builder.setBolt("jmsProducerBolt", new JMSProducerBolt(), 5)
                .shuffleGrouping("jmsConsumerSpout");
        Config conf = new Config();
        conf.setDebug(false);

        // If there are arguments, we are running on cluster, otherwise locally
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("jmsProxy", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
