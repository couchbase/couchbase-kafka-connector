/**
 * Copyright (C) 2015 Couchbase, Inc.
 * <p/>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p/>
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * <p/>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.kafka;

import com.couchbase.client.core.env.DefaultCoreEnvironment;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author Sergey Avseyev
 */
public class DefaultCouchbaseKafkaEnvironment extends DefaultCoreEnvironment implements CouchbaseKafkaEnvironment {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseKafkaEnvironment.class);

    private static final String KAFKA_KEY_SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private static final String KAFKA_VALUE_SERIALIZER_CLASS = "com.couchbase.kafka.coder.JsonEncoder";
    private static final String KAFKA_FILTER_CLASS = "com.couchbase.kafka.filter.MutationsFilter";
    private static final int KAFKA_EVENT_BUFFER_SIZE = 16384;
    private static final String KAFKA_ZOOKEEPER_ADDRESS = "127.0.0.1:2181";
    private static final String KAFKA_TOPIC = "default";
    private static final String COUCHBASE_STATE_SERIALIZER_CLASS = "com.couchbase.kafka.state.ZookeeperStateSerializer";
    private static final long COUCHBASE_STATE_SERIALIZATION_THRESHOLD = 2;
    private static final String COUCHBASE_BUCKET = "default";
    private static final String COUCHBASE_PASSWORD = "";
    private static final String COUCHBASE_NODE = "127.0.0.1";


    private final String kafkaKeySerializerClass;
    private final String kafkaFilterClass;
    private final String kafkaValueSerializerClass;
    private final int kafkaEventBufferSize;
    private String kafkaTopic;
    private String kafkaZookeeperAddress;
    private String couchbaseStateSerializerClass;
    private long couchbaseStateSerializationThreshold;
    private String couchbasePassword;
    private String couchbaseBucket;
    private List<String> couchbaseNodes;


    public static String SDK_PACKAGE_NAME_AND_VERSION = "couchbase-kafka-connector";
    private static final String VERSION_PROPERTIES = "com.couchbase.kafka.properties";

    /**
     * Sets up the package version and user agent.
     *
     * Note that because the class loader loads classes on demand, one class from the package
     * is loaded upfront.
     */
    static {
        try {
            Class<CouchbaseKafkaConnector> connectorClass = CouchbaseKafkaConnector.class;
            if (connectorClass == null) {
                throw new IllegalStateException("Could not locate CouchbaseKafkaConnector");
            }

            String version = null;
            String gitVersion = null;
            try {
                Properties versionProp = new Properties();
                versionProp.load(DefaultCoreEnvironment.class.getClassLoader().getResourceAsStream(VERSION_PROPERTIES));
                version = versionProp.getProperty("specificationVersion");
                gitVersion = versionProp.getProperty("implementationVersion");
            } catch (Exception e) {
                LOGGER.info("Could not retrieve version properties, defaulting.", e);
            }
            SDK_PACKAGE_NAME_AND_VERSION = String.format("couchbase-kafka-connector/%s (git: %s)",
                    version == null ? "unknown" : version, gitVersion == null ? "unknown" : gitVersion);

            // this will overwrite the USER_AGENT in Core
            // making core send user_agent with kafka connector version information
            USER_AGENT = String.format("%s (%s/%s %s; %s %s)",
                    SDK_PACKAGE_NAME_AND_VERSION,
                    System.getProperty("os.name"),
                    System.getProperty("os.version"),
                    System.getProperty("os.arch"),
                    System.getProperty("java.vm.name"),
                    System.getProperty("java.runtime.version")
            );
        } catch (Exception ex) {
            LOGGER.info("Could not set up user agent and packages, defaulting.", ex);
        }
    }

    /**
     * Creates a {@link CouchbaseKafkaEnvironment} with default settings applied.
     *
     * @return a {@link DefaultCouchbaseKafkaEnvironment} with default settings.
     */
    public static DefaultCouchbaseKafkaEnvironment create() {
        return new DefaultCouchbaseKafkaEnvironment(builder());
    }

    /**
     * Returns the {@link Builder} to customize environment settings.
     *
     * @return the {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    protected DefaultCouchbaseKafkaEnvironment(final Builder builder) {
        super(builder);

        if (!dcpEnabled()) {
            throw new IllegalStateException("Kafka integration cannot work without DCP enabled.");
        }

        kafkaKeySerializerClass = stringPropertyOr("kafka.keySerializerClass", builder.kafkaKeySerializerClass);
        kafkaValueSerializerClass = stringPropertyOr("kafka.valueSerializerClass", builder.kafkaValueSerializerClass);
        kafkaFilterClass = stringPropertyOr("kafka.filterClass", builder.kafkaFilterClass);
        kafkaEventBufferSize = intPropertyOr("kafka.eventBufferSize", builder.kafkaEventBufferSize);
        kafkaTopic = stringPropertyOr("kafka.topic", builder.kafkaTopic);
        kafkaZookeeperAddress = stringPropertyOr("kafka.zookeeperAddress", builder.kafkaZookeeperAddress);
        couchbaseStateSerializerClass = stringPropertyOr("couchbaseStateSerializerClass", builder.couchbaseStateSerializerClass);
        couchbaseStateSerializationThreshold = longPropertyOr("couchbaseStateSerializationThreshold", builder.couchbaseStateSerializationThreshold);
        couchbaseNodes = stringListPropertyOr("couchbase.nodes", builder.couchbaseNodes);
        couchbaseBucket = stringPropertyOr("couchbase.bucket", builder.couchbaseBucket);
        couchbasePassword = stringPropertyOr("couchbase.password", builder.couchbasePassword);
    }

    @Override
    public String kafkaValueSerializerClass() {
        return kafkaValueSerializerClass;
    }

    @Override
    public String kafkaKeySerializerClass() {
        return kafkaKeySerializerClass;
    }

    @Override
    public String kafkaFilterClass() {
        return kafkaFilterClass;
    }

    @Override
    public int kafkaEventBufferSize() {
        return kafkaEventBufferSize;
    }

    @Override
    public String kafkaZookeeperAddress() {
        return kafkaZookeeperAddress;
    }

    @Override
    public String kafkaTopic() {
        return kafkaTopic;
    }

    @Override
    public long couchbaseStateSerializationThreshold() {
        return couchbaseStateSerializationThreshold;
    }

    @Override
    public String couchbaseStateSerializerClass() {
        return couchbaseStateSerializerClass;
    }

    @Override
    public List<String> couchbaseNodes() {
        return couchbaseNodes;
    }

    @Override
    public String couchbaseBucket() {
        return couchbaseBucket;
    }

    @Override
    public String couchbasePassword() {
        return couchbasePassword;
    }

    private List<String> stringListPropertyOr(String path, List<String> def) {
        String found = stringPropertyOr(path, null);
        if (found == null) {
            return def;
        } else {
            return Arrays.asList(found.split(";"));
        }
    }

    @Override
    protected StringBuilder dumpParameters(StringBuilder sb) {
        //first dump core's parameters
        super.dumpParameters(sb);
        //dump kafka-connector specific parameters
        sb.append(", kafkaKeySerializerClass=").append(this.kafkaKeySerializerClass);
        sb.append(", kafkaFilterClass=").append(this.kafkaFilterClass);
        sb.append(", kafkaValueSerializerClass=").append(this.kafkaValueSerializerClass);
        sb.append(", kafkaEventBufferSize=").append(this.kafkaEventBufferSize);
        sb.append(", kafkaTopic=").append(this.kafkaTopic);
        sb.append(", kafkaZookeeperAddress=").append(this.kafkaZookeeperAddress);
        sb.append(", couchbaseStateSerializerClass=").append(this.couchbaseStateSerializerClass);
        sb.append(", couchbaseStateSerializationThreshold=").append(this.couchbaseStateSerializationThreshold);
        sb.append(", couchbaseBucket=").append(this.couchbaseBucket);
        sb.append(", couchbaseNodes=").append(String.join(",", this.couchbaseNodes));

        return sb;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("CouchbaseKafkaEnvironment: {");
        this.dumpParameters(sb).append('}');
        return sb.toString();
    }

    public static class Builder extends DefaultCoreEnvironment.Builder {
        private String kafkaKeySerializerClass = KAFKA_KEY_SERIALIZER_CLASS;
        private String kafkaValueSerializerClass = KAFKA_VALUE_SERIALIZER_CLASS;
        private int kafkaEventBufferSize = KAFKA_EVENT_BUFFER_SIZE;
        private String kafkaFilterClass = KAFKA_FILTER_CLASS;
        private String kafkaTopic = KAFKA_TOPIC;
        private String kafkaZookeeperAddress = KAFKA_ZOOKEEPER_ADDRESS;
        private String couchbaseStateSerializerClass = COUCHBASE_STATE_SERIALIZER_CLASS;
        private List<String> couchbaseNodes;
        private String couchbaseBucket = COUCHBASE_BUCKET;
        private String couchbasePassword = COUCHBASE_PASSWORD;
        private long couchbaseStateSerializationThreshold = COUCHBASE_STATE_SERIALIZATION_THRESHOLD;

        public Builder() {
            couchbaseNodes = Collections.singletonList(COUCHBASE_NODE);
        }

        public Builder kafkaValueSerializerClass(final String className) {
            this.kafkaValueSerializerClass = className;
            return this;
        }

        public Builder kafkaKeySerializerClass(final String className) {
            this.kafkaKeySerializerClass = className;
            return this;
        }

        public Builder kafkaFilterClass(final String className) {
            this.kafkaFilterClass = className;
            return this;
        }

        public Builder kafkaEventBufferSize(final int eventBufferSize) {
            this.kafkaEventBufferSize = eventBufferSize;
            return this;
        }

        public Builder kafkaTopic(final String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
            return this;
        }

        public Builder kafkaZookeeperAddress(final String kafkaZookeeperAddress) {
            this.kafkaZookeeperAddress = kafkaZookeeperAddress;
            return this;
        }

        public Builder couchbaseStateSerializerClass(final String couchbaseStateSerializerClass) {
            this.couchbaseStateSerializerClass = couchbaseStateSerializerClass;
            return this;
        }

        public Builder couchbaseStateSerializationThreshold(final long couchbaseStateSerializationThreshold) {
            this.couchbaseStateSerializationThreshold = couchbaseStateSerializationThreshold;
            return this;
        }

        public Builder couchbaseNodes(final List<String> couchbaseNodes) {
            this.couchbaseNodes = couchbaseNodes;
            return this;
        }

        public Builder couchbaseNodes(final String couchbaseNode) {
            this.couchbaseNodes(Collections.singletonList(couchbaseNode));
            return this;
        }

        public Builder couchbaseBucket(final String couchbaseBucket) {
            this.couchbaseBucket = couchbaseBucket;
            return this;
        }

        public Builder couchbasePassword(final String couchbasePassword) {
            this.couchbasePassword = couchbasePassword;
            return this;
        }

        @Override
        public DefaultCouchbaseKafkaEnvironment build() {
            return new DefaultCouchbaseKafkaEnvironment(this);
        }
    }
}
