package com.couchbase.kafka;

import com.couchbase.client.core.env.CoreEnvironment;

import java.util.List;

/**
 * A {@link CouchbaseKafkaEnvironment} settings related to Kafka connection, in addition to all the core building blocks
 * like environment settings and thread pools inherited from {@link CoreEnvironment} so
 * that the application can work with it properly.
 *
 * This interface defines the contract. How properties are loaded is chosen by the implementation. See the
 * {@link DefaultCouchbaseKafkaEnvironment} class for the default implementation.
 *
 * Note that the {@link CouchbaseKafkaEnvironment} is stateful, so be sure to call {@link CoreEnvironment#shutdown()}
 * properly.
 *
 * @author Sergey Avseyev
 */
public interface CouchbaseKafkaEnvironment extends CoreEnvironment {
    /**
     * Full name of class used to encode objects to byte[] to store in Kafka. It have to implement
     * {@link kafka.serializer.Encoder} parametrized with DCPEvent.
     *
     * @return class name of encoder
     */
    String kafkaValueSerializerClass();

    /**
     * Full name of class used to encode object keys to byte[] to store in Kafka. It have to implement
     * {@link kafka.serializer.Encoder} parametrized with String.
     *
     * @return class name of encoder
     */
    String kafkaKeySerializerClass();

    /**
     * Full name of class used to filter data stream from Couchbase. It have to implement
     * {@link com.couchbase.kafka.filter.Filter}.
     *
     * @return class name of filter
     */
    String kafkaFilterClass();

    /**
     * Returns the size of the events ringbuffer.
     *
     * @return the size of the ringbuffer.
     */
    int kafkaEventBufferSize();

    /**
     * Full name of class used to serialize state of the Couchbase streams. It have to
     * implement {@link com.couchbase.kafka.state.StateSerializer}.
     *
     * @return class name of the serializer
     */
    String couchbaseStateSerializerClass();

    /**
     * Minimum time between dumping the state
     *
     * @return time in milliseconds
     */
    long couchbaseStateSerializationThreshold();

    /**
     * List of Couchbase nodes used to connect.
     *
     * @return list of node addresses
     */
    List<String> couchbaseNodes();

    /**
     * Name of the bucket in Couchbase.
     *
     * @return name of the bucket
     */
    String couchbaseBucket();

    /**
     * Password if the bucket is protected.
     *
     * @return couchbase password.
     */
    String couchbasePassword();

    /**
     * Zookeeper address to pass into kafka client.
     *
     * @return zookeeper node address.
     */
    String kafkaZookeeperAddress();

    /**
     * Kafka topic to post events.
     *
     * @return kafka topic name.
     */
    String kafkaTopic();
}
