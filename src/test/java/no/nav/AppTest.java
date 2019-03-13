package no.nav;

import static org.junit.jupiter.api.Assertions.assertEquals;
import com.google.common.base.Charsets;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.ProducedKafkaRecord;
import com.salesforce.kafka.test.junit5.SharedKafkaTestResource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Unit test for simple App.
 */
public class AppTest 
{

    /**
     * We have a single embedded Kafka server that gets started when this test class is initialized.
     *
     * It's automatically started before any methods are run via the @RegisterExtension annotation.
     * It's automatically stopped after all of the tests are completed via the @RegisterExtension annotation.
     */
    @RegisterExtension
    public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();



    /**
     * Before every test, we generate a random topic name and create it within the embedded kafka server.
     * Each test can then be segmented run any tests against its own topic.
     */
    private String topicName;


    /**
     * This happens once before every test method.
     * Create a new topic with unique name.
     */
    @BeforeEach
    void beforeTest() {
        // Generate unique topic name
        topicName = getClass().getSimpleName() + Clock.systemUTC().millis();

        // Create topic with 3 partitions,
        // NOTE: This will create partition ids 0 through 2, because partitions are indexed at 0 :)
        getKafkaTestUtils().createTopic(topicName, 3, (short) 1);
    }

    /**
     * Sometimes you don't care what the contents of the records you produce or consume are.
     */
    @Test
    void testProducerAndConsumerUtils() {
        final int numberOfRecords = 10;
        final int partitionId = 2;

        // Create our utility class
        final KafkaTestUtils kafkaTestUtils = getKafkaTestUtils();

        // Produce some random records
        final List<ProducedKafkaRecord<byte[], byte[]>> producedRecordsList =
                kafkaTestUtils.produceRecords(numberOfRecords, topicName, partitionId);

        // You can get details about what got produced into Kafka, including the partition and offset for each message.
        for (final ProducedKafkaRecord<byte[], byte[]> producedKafkaRecord: producedRecordsList) {
            // This is the key of the message that was produced.
            final String key = new String(producedKafkaRecord.getKey(), Charsets.UTF_8);

            // This is the value of the message that was produced.
            final String value = new String(producedKafkaRecord.getValue(), Charsets.UTF_8);

            // Other details about topic, partition, and offset it was written onto.
            final String topic = producedKafkaRecord.getTopic();
            final int partition = producedKafkaRecord.getPartition();
            final long offset = producedKafkaRecord.getOffset();

            // for debugging
            //logger.info("Produced into topic:{} partition:{} offset:{} key:{} value:{}", topic, partition, offset, key, value);
        }

        // Now to consume all records from partition 2 only.
        final List<ConsumerRecord<String, String>> consumerRecords = kafkaTestUtils.consumeAllRecordsFromTopic(
                topicName,
                Collections.singleton(partitionId),
                StringDeserializer.class,
                StringDeserializer.class
        );

        // Validate
        assertEquals(numberOfRecords, consumerRecords.size(), "Should have 10 records");

        final Iterator<ConsumerRecord<String, String>> consumerRecordIterator = consumerRecords.iterator();
        final Iterator<ProducedKafkaRecord<byte[], byte[]>> producedKafkaRecordIterator = producedRecordsList.iterator();

        while (consumerRecordIterator.hasNext()) {
            final ConsumerRecord<String, String> consumerRecord = consumerRecordIterator.next();
            final ProducedKafkaRecord<byte[], byte[]> producedKafkaRecord = producedKafkaRecordIterator.next();

            final String expectedKey = new String(producedKafkaRecord.getKey(), Charsets.UTF_8);
            final String expectedValue = new String(producedKafkaRecord.getValue(), Charsets.UTF_8);
            final String actualKey = consumerRecord.key();
            final String actualValue = consumerRecord.value();

            // Make sure they match
            assertEquals(producedKafkaRecord.getTopic(), consumerRecord.topic(), "Has correct topic");
            assertEquals(producedKafkaRecord.getPartition(), consumerRecord.partition(), "Has correct partition");
            assertEquals(producedKafkaRecord.getOffset(), consumerRecord.offset(), "Has correct offset");
            assertEquals(expectedKey, actualKey, "Has correct key");
            assertEquals(expectedValue, actualValue, "Has correct value");
        }
    }

    /**
     * Simple accessor.
     */
    private KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }
}
