/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
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
import com.couchbase.client.java.AsyncCluster;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.TestZKUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * @author Sergey Avseyev
 */
public class CouchbaseProducerTest {
    /*
    private int brokerId = 0;
    private String topicName = "test";
    private String bucketName = "kafka";
    private int brokerPort;
    private KafkaServer kafkaServer;
    private ZkClient zkClient;
    private EmbeddedZookeeper zkServer;
    private Bucket bucket;
    private KafkaEnvironment env;

    @Before
    public void setup() {
        // setup Zookeeper
        String zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), 30000, 30000, ZKStringSerializer$.MODULE$);

        // setup Broker
        brokerPort = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, brokerPort, false);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);

        // create topicName
        AdminUtils.createTopic(zkClient, topicName, 1, 1, new Properties());

        List<KafkaServer> servers = new ArrayList<KafkaServer>();
        servers.add(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topicName, 0, 5000);

        CouchbaseCluster cluster = CouchbaseCluster.create();
        ClusterManager clusterManager = cluster.clusterManager("Administrator", "password");
        boolean exists = clusterManager.hasBucket(bucketName);

        if (!exists) {
            clusterManager.insertBucket(DefaultBucketSettings
                    .builder()
                    .name(bucketName)
                    .quota(256)
                    .enableFlush(true)
                    .type(BucketType.COUCHBASE)
                    .build());
        }

        bucket = cluster.openBucket(bucketName, "");
        BucketManager bucketManager = bucket.bucketManager();
        bucketManager.flush();

        env = DefaultKafkaEnvironment.builder()
                .dcpEnabled(true)
                .build();
    }

    @After
    public void teardown() {
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    @Test
    public void producerTest() throws InterruptedException {
        List<String> couchbaseNodes = new ArrayList<>();
        couchbaseNodes.add("locahost:8091");
        CouchbaseProducer producer = new CouchbaseProducer(
                couchbaseNodes,
                "default",
                topicName,
                zkServer.connectString(),
                env
        );

        bucket.insert(StringDocument.create("foo", "bar"));
        producer.run();
    }
    */
}