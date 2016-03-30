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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.deps.com.lmax.disruptor.ExceptionHandler;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.client.deps.com.lmax.disruptor.dsl.Disruptor;
import com.couchbase.client.deps.io.netty.util.concurrent.DefaultThreadFactory;
import com.couchbase.kafka.filter.Filter;
import com.couchbase.kafka.state.ConnectorState;
import com.couchbase.kafka.state.StateSerializer;
import com.couchbase.kafka.state.StreamState;
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.Iterator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link CouchbaseKafkaConnector} is an entry point of the library. It sets up connections with both Couchbase and
 * Kafka clusters. And carries all events from Couchbase to Kafka.
 *
 * The example below will transfer all mutations from Couchbase bucket "my-bucket" as JSON to Kafka topic "my-topic".
 * <pre>
 * {@code
 *  DefaultCouchbaseKafkaEnvironment.Builder builder =
 *        (DefaultCouchbaseKafkaEnvironment.Builder) DefaultCouchbaseKafkaEnvironment.builder()
 *           .kafkaFilterClass("kafka.serializer.StringEncoder")
 *           .kafkaValueSerializerClass("com.couchbase.kafka.coder.JsonEncoder")
 *           .dcpEnabled(true);
 *  CouchbaseKafkaEnvironment env = builder.build();
 *  CouchbaseKafkaConnector connector = CouchbaseKafkaConnector.create(env,
 *                 "couchbase.example.com", "my-bucket", "pass",
 *                 "kafka.example.com", "my-topic");
 *  connector.run();
 * }
 * </pre>
 *
 * @author Sergey Avseyev
 */
public class CouchbaseKafkaConnector implements Runnable {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseKafkaConnector.class);

    private static final DCPEventFactory DCP_EVENT_FACTORY = new DCPEventFactory();

    private final ClusterFacade core;
    private final ExecutorService disruptorExecutor;
    private final Disruptor<DCPEvent> disruptor;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final KafkaWriter kafkaWriter;
    private final Producer<String, DCPEvent> producer;
    private final CouchbaseReader couchbaseReader;
    private final Filter filter;
    private final StateSerializer stateSerializer;
    private final CouchbaseKafkaEnvironment environment;

    /**
     * Create {@link CouchbaseKafkaConnector} with specified settings (list of Couchbase nodes)
     * and custom {@link CouchbaseKafkaEnvironment}.
     *
     * @param couchbaseNodes    address of Couchbase node to override {@link CouchbaseKafkaEnvironment#couchbaseNodes()}.
     * @param couchbaseBucket   name of Couchbase bucket to override {@link CouchbaseKafkaEnvironment#couchbaseBucket()}.
     * @param couchbasePassword password for Couchbase bucket to override {@link CouchbaseKafkaEnvironment#couchbasePassword()}.
     * @param kafkaZookeeper    address of Zookeeper to override {@link CouchbaseKafkaEnvironment#kafkaZookeeperAddress()}.
     * @param kafkaTopic        name of Kafka topic to override {@link CouchbaseKafkaEnvironment#kafkaTopic()}.
     * @param environment       custom environment object.
     */
    private CouchbaseKafkaConnector(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                                    final String kafkaZookeeper, final String kafkaTopic, final CouchbaseKafkaEnvironment environment) {
        try {
            filter = (Filter) Class.forName(environment.kafkaFilterClass()).newInstance();
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Cannot initialize filter class:" + environment.kafkaFilterClass(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot initialize filter class:" + environment.kafkaFilterClass(), e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot initialize filter class:" + environment.kafkaFilterClass(), e);
        }
        try {
            stateSerializer = (StateSerializer) Class.forName(environment.couchbaseStateSerializerClass())
                    .getDeclaredConstructor(CouchbaseKafkaEnvironment.class)
                    .newInstance(environment);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Cannot initialize serializer class:" + environment.kafkaFilterClass(), e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException("Cannot initialize serializer class:" + environment.kafkaFilterClass(), e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException("Cannot initialize serializer class:" + environment.kafkaFilterClass(), e);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException("Cannot initialize serializer class:" + environment.kafkaFilterClass(), e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Cannot initialize serializer class:" + environment.kafkaFilterClass(), e);
        }
        this.environment = environment;

        core = new CouchbaseCore(environment);
        disruptorExecutor = Executors.newFixedThreadPool(2, new DefaultThreadFactory("cb-kafka", true));
        disruptor = new Disruptor<DCPEvent>(
                DCP_EVENT_FACTORY,
                environment.kafkaEventBufferSize(),
                disruptorExecutor
        );
        disruptor.handleExceptionsWith(new ExceptionHandler() {
            @Override
            public void handleEventException(final Throwable ex, final long sequence, final Object event) {
                LOGGER.warn("Exception while Handling DCP Events {}", event, ex);
            }

            @Override
            public void handleOnStartException(final Throwable ex) {
                LOGGER.warn("Exception while Starting DCP RingBuffer", ex);
            }

            @Override
            public void handleOnShutdownException(final Throwable ex) {
                LOGGER.info("Exception while shutting down DCP RingBuffer", ex);
            }
        });

        final Properties props = new Properties();
        ZkClient zkClient = new ZkClient(kafkaZookeeper, 4000, 6000, ZKStringSerializer$.MODULE$);
        List<String> brokerList = new ArrayList<String>();
        Iterator<Broker> brokerIterator = ZkUtils.getAllBrokersInCluster(zkClient).iterator();
        while (brokerIterator.hasNext()) {
            Broker broker = brokerIterator.next();
            String brokerAddress = broker.host() + ":" + broker.port();
            brokerList.add(brokerAddress);
        }

        props.put("metadata.broker.list", joinNodes(brokerList));
        props.put("serializer.class", environment.kafkaValueSerializerClass());
        props.put("key.serializer.class", environment.kafkaKeySerializerClass());
        final ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<String, DCPEvent>(producerConfig);

        kafkaWriter = new KafkaWriter(kafkaTopic, environment, producer, filter);
        disruptor.handleEventsWith(kafkaWriter);
        disruptor.start();
        dcpRingBuffer = disruptor.getRingBuffer();
        couchbaseReader = new CouchbaseReader(couchbaseNodes, couchbaseBucket, couchbasePassword, core,
                environment, dcpRingBuffer, stateSerializer);
        couchbaseReader.connect();
    }

    /**
     * Creates {@link CouchbaseKafkaConnector} with default settings. Like using "localhost" as endpoints,
     * "default" Couchbase bucket and Kafka topic.
     *
     * @return {@link CouchbaseKafkaConnector} with default settings
     */
    public static CouchbaseKafkaConnector create() {
        DefaultCouchbaseKafkaEnvironment.Builder builder = DefaultCouchbaseKafkaEnvironment.builder();
        builder.dcpEnabled(true);
        return create(builder.build());
    }

    /**
     * Create {@link CouchbaseKafkaConnector} with specified settings.
     *
     * @param environment custom environment object
     * @return configured {@link CouchbaseKafkaConnector}
     */
    public static CouchbaseKafkaConnector create(final CouchbaseKafkaEnvironment environment) {
        return create(environment.couchbaseNodes(), environment.couchbaseBucket(), environment.couchbasePassword(),
                environment.kafkaZookeeperAddress(), environment.kafkaTopic(), environment);
    }

    /**
     * Create {@link CouchbaseKafkaConnector} with specified settings.
     *
     * @param couchbaseNodes    address of Couchbase node to override {@link CouchbaseKafkaEnvironment#couchbaseNodes()}.
     * @param couchbaseBucket   name of Couchbase bucket to override {@link CouchbaseKafkaEnvironment#couchbaseBucket()}.
     * @param couchbasePassword password for Couchbase bucket to override {@link CouchbaseKafkaEnvironment#couchbasePassword()}.
     * @param kafkaZookeeper    address of Zookeeper to override {@link CouchbaseKafkaEnvironment#kafkaZookeeperAddress()}.
     * @param kafkaTopic        name of Kafka topic to override {@link CouchbaseKafkaEnvironment#kafkaTopic()}.
     * @param environment       environment object
     * @return configured {@link CouchbaseKafkaConnector}
     */
    public static CouchbaseKafkaConnector create(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                                                 final String kafkaZookeeper, final String kafkaTopic, final CouchbaseKafkaEnvironment environment) {
        return new CouchbaseKafkaConnector(couchbaseNodes, couchbaseBucket, couchbasePassword, kafkaZookeeper, kafkaTopic, environment);
    }

    /**
     * Create {@link CouchbaseKafkaConnector} with specified settings.
     *
     * @param couchbaseNode     address of Couchbase node.
     * @param couchbaseBucket   name of Couchbase bucket.
     * @param couchbasePassword password for Couchbase bucket.
     * @param kafkaZookeeper    address of Zookeeper.
     * @param kafkaTopic        name of Kafka topic.
     * @return configured {@link CouchbaseKafkaConnector}
     * @deprecated Use {@link CouchbaseKafkaEnvironment} to initialize connector settings.
     */
    public static CouchbaseKafkaConnector create(final String couchbaseNode, final String couchbaseBucket, final String couchbasePassword,
                                                 final String kafkaZookeeper, final String kafkaTopic) {
        DefaultCouchbaseKafkaEnvironment.Builder builder = DefaultCouchbaseKafkaEnvironment.builder();
        builder.couchbaseNodes(Collections.singletonList(couchbaseNode))
                .couchbasePassword(couchbasePassword)
                .couchbaseBucket(couchbaseBucket)
                .kafkaZookeeperAddress(kafkaZookeeper)
                .kafkaTopic(kafkaTopic)
                .dcpEnabled(true);
        return create(builder.build());
    }

    /**
     * Initialize connector state for given partitions with current vbucketUUID and sequence number.
     *
     * @param partitions list of partitions related to the state.
     * @return the list of the objects representing sequence numbers
     */
    public ConnectorState currentState(int... partitions) {
        ConnectorState currentState = couchbaseReader.currentState();
        if (partitions.length == 0) {
            return currentState;
        }
        ConnectorState state = new ConnectorState();
        for (int partition : partitions) {
            state.put(currentState.get((short) partition));
        }
        return state;
    }

    /**
     * Initialize connector state for given partitions with current vbucketUUID and zero sequence number.
     *
     * Useful to stream from  or till current point of time.
     *
     * @param partitions list of partitions related to the state.
     * @return the list of the objects representing sequence numbers
     */
    public ConnectorState startState(int... partitions) {
        return overrideSequenceNumber(currentState(partitions), 0);
    }

    /**
     * Initialize connector state for given partitions with current vbucketUUID and maximum sequence number.
     *
     * Represents infinity for upper boundary.
     *
     * @param partitions list of partitions related to the state.
     * @return the list of the objects representing sequence numbers
     */
    public ConnectorState endState(int... partitions) {
        return overrideSequenceNumber(currentState(partitions), 0xffffffff);
    }

    /**
     * Initialize connector state for given partitions using configured serializer.
     *
     * @param partitions list of partitions related to the state.
     * @return the list of the objects representing sequence numbers
     */
    public ConnectorState loadState(int... partitions) {
        return stateSerializer.load(startState(partitions));
    }

    /**
     * Executes worker reading loop, which relays all events from Couchbase to Kafka.
     */
    @Override
    public void run() {
        run(startState(), endState());
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka in specified range.
     *
     * @param fromState starting state
     * @param toState   finishing state
     */
    public void run(final ConnectorState fromState, final ConnectorState toState) {
        couchbaseReader.run(fromState, toState);
    }

    private String joinNodes(final List<String> list) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String item : list) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(item);
        }
        return sb.toString();
    }

    private ConnectorState overrideSequenceNumber(ConnectorState connectorState, long sequenceNumber) {
        ConnectorState state = new ConnectorState();
        for (StreamState streamState : connectorState) {
            state.put(new StreamState(streamState.partition(), streamState.vbucketUUID(), sequenceNumber));
        }
        return state;
    }
}
