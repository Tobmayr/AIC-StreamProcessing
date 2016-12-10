package at.ac.tuwien.aic.streamprocessing.kafka.utils;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;


public class LocalKafkaInstance {
    private final static Logger logger = LoggerFactory.getLogger(LocalKafkaInstance.class);
    private final static String TMP_LOG_PREFIX = "aic-stream";

    private final int kafka_port;
    private final int zookeeper_port;

    private Path temporaryLogDir;

    private TestingServer zookeeperServer;
    private KafkaServerStartable kafkaServer;

    public LocalKafkaInstance(int kafka_port, int zookeeper_port) {
        this.kafka_port = kafka_port;
        this.zookeeper_port = zookeeper_port;

        try {
            temporaryLogDir = Files.createTempDirectory(TMP_LOG_PREFIX);
        } catch (IOException e) {
            logger.error("Failed to create temporary zookeeper and kafka log directory. Aborting", e);
            throw new RuntimeException(e);
        }
    }

    public static LocalKafkaInstance createDefault() {
        return new LocalKafkaInstance(9092, 2181);
    }

    public String getConnectString() {
        return zookeeperServer.getConnectString();
    }

    public String getKafkaConnectString() {
        return "localhost:" + kafka_port;
    }

    public void start() throws Exception {
        System.out.println("Starting zookeeper.");
        File zookeeperLogDir = temporaryLogDir.resolve("zookeeper-logs").toFile();
        this.zookeeperServer = new TestingServer(zookeeper_port, zookeeperLogDir);
        System.out.println("Started zookeeper.");

        ExponentialBackoffRetry retryPolicy = new BoundedExponentialBackoffRetry(200, 5000, 5);
        CuratorFramework zookeeper = CuratorFrameworkFactory.newClient(
                zookeeperServer.getConnectString(), retryPolicy);
        zookeeper.start();

        String kafkaLogDir = temporaryLogDir.resolve("kafka-logs").toString();

        Properties p = new Properties();
        p.setProperty("zookeeper.connect", zookeeperServer.getConnectString());
        p.setProperty("broker.id", "0");
        p.setProperty("port", "" + kafka_port);
        p.setProperty("delete.topic.enable", "true");
        p.setProperty("log.dir", kafkaLogDir);
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

        FileUtils.deleteDirectory(temporaryLogDir.toFile());
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
}
