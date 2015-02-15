package com.couchbase.kafka;

import com.couchbase.client.core.env.CoreEnvironment;

/**
 * @author Sergey Avseyev
 */
public interface CouchbaseKafkaEnvironment extends CoreEnvironment {
    String kafkaValueSerializerClass();
    String kafkaKeySerializerClass();
    String kafkaFilterClass();
    int kafkaEventBufferSize();
}
