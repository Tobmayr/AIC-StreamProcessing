package at.ac.tuwien.aic.streamprocessing.kafka;

import at.ac.tuwien.aic.streamprocessing.kafka.utils.LocalKafkaInstance;
import org.junit.After;
import org.junit.Before;

import java.util.List;


public abstract class AbstractLocalKafkaInstanceTest {

    public static final int DEFAULT_TEST_TIMEOUT = 20000;

    private List<String> topics;
    private LocalKafkaInstance kafkaInstance;

    public AbstractLocalKafkaInstanceTest(List<String> topics) {
        this.topics = topics;
    }

    @Before
    public void setup() throws Exception {
        kafkaInstance = LocalKafkaInstance.createDefault();
        kafkaInstance.start();

        for (String topic : topics) {
            kafkaInstance.createTopic(topic);
        }
    }

    @After
    public void teardown() throws Exception {
        kafkaInstance.stop();
    }

    protected LocalKafkaInstance getKafkaInstance() {
        return kafkaInstance;
    }
}
