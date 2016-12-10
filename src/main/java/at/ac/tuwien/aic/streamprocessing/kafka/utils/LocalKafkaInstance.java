package at.ac.tuwien.aic.streamprocessing.kafka.utils;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;


public class LocalKafkaInstance {
    private final static Logger logger = LoggerFactory.getLogger(LocalKafkaInstance.class);

    private final int kafka_port;
    private final int zookeeper_port;
    /*private static final String KAFKA_URI = "localhost:" + kafka_port;
    private static final String ZOOKEEPER_URI = "127.0.0.1:" + zookeeper_port;*/

    private TestingServer zookeeperServer;
    private KafkaServerStartable kafkaServer;

    public LocalKafkaInstance(int kafka_port, int zookeeper_port) {
        this.kafka_port = kafka_port;
        this.zookeeper_port = zookeeper_port;
    }

    public String getConnectString() {
        return zookeeperServer.getConnectString();
    }

    public String getKafkaConnectString() {
        return "localhost:" + kafka_port;
    }

    public void start() throws Exception {
        System.out.println("Starting zookeeper.");
        this.zookeeperServer = new TestingServer(zookeeper_port, new File("/tmp/zookeeper-logs"));
        System.out.println("Started zookeeper.");

        ExponentialBackoffRetry retryPolicy = new BoundedExponentialBackoffRetry(200, 5000, 5);
        CuratorFramework zookeeper = CuratorFrameworkFactory.newClient(
                zookeeperServer.getConnectString(), retryPolicy);
        zookeeper.start();

        Properties p = new Properties();
        p.setProperty("zookeeper.connect", zookeeperServer.getConnectString());
        p.setProperty("broker.id", "0");
        p.setProperty("port", "" + kafka_port);
        p.setProperty("delete.topic.enable", "true");
        p.setProperty("log.dir", "/tmp/kafka-logs");
        p.setProperty("log.retention.ms", "30000"); // 30 seconds retention
        KafkaConfig config = new KafkaConfig(p);

        this.kafkaServer = new KafkaServerStartable(config);

        System.out.println("Starting kafka");
        kafkaServer.startup();
        System.out.println("Started kafka");
    }

    public void stop() throws Exception {
        System.out.println("Shutting down kafka.");
        this.kafkaServer.shutdown();
        System.out.println("Shut down kafka.");

        System.out.println("Shutting down zookeeper");
        this.zookeeperServer.close();
        this.zookeeperServer.stop();
        System.out.println("Shut down zookeeper");
    }

    public void createTopic(String topicName) {
        System.out.println("Creating topic " + topicName);

        String zookeeper_uri = "localhost:" + zookeeper_port;

        ZkClient zkClient = new ZkClient(zookeeper_uri, 5000, 5000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeper_uri), false);

        int partitions = 1;
        int replications = 1;
        AdminUtils.createTopic(zkUtils, topicName, partitions, replications, AdminUtils.createTopic$default$5(),
                AdminUtils.createTopic$default$6());

        zkUtils.close();

        System.out.println("Created topic " + topicName);
    }

    /*public static Properties createProducerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_URI);
        properties.put("acks", "1");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return properties;
    }

    public static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_URI);
        properties.put("group.id", "test-group-id");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "10000");
        properties.put("auto.offset.reset", "earliest"); // from-beginning
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }*/
}
