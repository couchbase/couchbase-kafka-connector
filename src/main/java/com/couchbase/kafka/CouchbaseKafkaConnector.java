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
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Sergey Avseyev
 */
public class CouchbaseKafkaConnector implements Runnable {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseKafkaConnector.class);

    private static final String DEFAULT_BUCKET = "default";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_TOPIC = "default";
    private static final String DEFAULT_COUCHBASE_NODE = "127.0.0.1";
    private static final String DEFAULT_ZOOKEEPER_NODE = "127.0.0.1:2181";

    private static final DCPEventFactory DCP_EVENT_FACTORY = new DCPEventFactory();

    private final ClusterFacade core;
    private final ExecutorService disruptorExecutor;
    private final Disruptor<DCPEvent> disruptor;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final KafkaWriter kafkaWriter;
    private final Producer<String, DCPEvent> producer;
    private final CouchbaseReader couchbaseReader;
    private final Filter filter;

    public static CouchbaseKafkaConnector create() {
        return create(DEFAULT_COUCHBASE_NODE, DEFAULT_BUCKET, DEFAULT_PASSWORD, DEFAULT_ZOOKEEPER_NODE, DEFAULT_TOPIC);
    }

    public static CouchbaseKafkaConnector create(final String couchbaseNode, final String couchbaseBucket, final String couchbasePassword,
                                                 final String kafkaZookeeper, final String kafkaTopic) {

        return create(((DefaultCouchbaseKafkaEnvironment.Builder) DefaultCouchbaseKafkaEnvironment.builder().dcpEnabled(true)).build(),
                couchbaseNode, couchbaseBucket, couchbasePassword, kafkaZookeeper, kafkaTopic);
    }

    public static CouchbaseKafkaConnector create(final CouchbaseKafkaEnvironment environment,
                                                 final String couchbaseNode, final String couchbaseBucket, final String couchbasePassword,
                                                 final String kafkaZookeeper, final String kafkaTopic) {
        return new CouchbaseKafkaConnector(environment, Collections.singletonList(couchbaseNode),
                couchbaseBucket, couchbasePassword, kafkaZookeeper, kafkaTopic);
    }

    private CouchbaseKafkaConnector(final CouchbaseKafkaEnvironment environment,
                                    final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                                    final String kafkaZookeeper, final String kafkaTopic) {
        try {
            filter = (Filter) Class.forName(environment.kafkaFilterClass()).newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Cannot initialize filter class", e);
        }
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
                LOGGER.warn("Exception while Handling DCP Events {}, {}", event, ex);
            }

            @Override
            public void handleOnStartException(final Throwable ex) {
                LOGGER.warn("Exception while Starting DCP RingBuffer {}", ex);
            }

            @Override
            public void handleOnShutdownException(final Throwable ex) {
                LOGGER.info("Exception while shutting down DCP RingBuffer {}", ex);
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

        kafkaWriter = new KafkaWriter(kafkaTopic, producer, filter);
        disruptor.handleEventsWith(kafkaWriter);
        disruptor.start();
        dcpRingBuffer = disruptor.getRingBuffer();
        couchbaseReader = new CouchbaseReader(core, dcpRingBuffer, couchbaseNodes, couchbaseBucket, couchbasePassword);
        couchbaseReader.connect();
    }

    @Override
    public void run() {
        couchbaseReader.run();
    }

    private String joinNodes(final List<String> list) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (String item : list) {
            if (first) {
                first = false;
            } else {
                sb.append(";");
            }
            sb.append(item);
        }
        return sb.toString();
    }
}
