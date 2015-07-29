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

package com.couchbase.kafka.state;

import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.kafka.CouchbaseKafkaEnvironment;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * @author Sergey Avseyev
 */
public class ZookeeperStateSerializer implements StateSerializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperStateSerializer.class);
    private final ZkClient zkClient;
    private final String bucket;

    public ZookeeperStateSerializer(final CouchbaseKafkaEnvironment environment) {
        this.zkClient = new ZkClient(environment.kafkaZookeeperAddress(), 4000, 6000, ZKStringSerializer$.MODULE$);
        this.bucket = environment.couchbaseBucket();
    }

    @Override
    public void dump(BucketStreamAggregatorState aggregatorState) {
        for (int partition = 0; partition < aggregatorState.numPartitions(); partition++) {
            dump(aggregatorState, partition);
        }
    }

    @Override
    public void dump(BucketStreamAggregatorState aggregatorState, int partition) {
        final BucketStreamState streamState = aggregatorState.get(partition);
        ObjectNode json = MAPPER.createObjectNode();
        json.put("vbucketUUID", streamState.vbucketUUID());
        json.put("startSequenceNumber", streamState.startSequenceNumber());
        json.put("endSequenceNumber", streamState.endSequenceNumber());
        json.put("snapshotStartSequenceNumber", streamState.snapshotStartSequenceNumber());
        json.put("snapshotEndSequenceNumber", streamState.snapshotEndSequenceNumber());
        zkClient.createPersistent(pathForState(partition), true);
        zkClient.writeData(pathForState(partition), json.toString());
    }

    @Override
    public BucketStreamAggregatorState load(BucketStreamAggregatorState aggregatorState) {
        for (int partition = 0; partition < aggregatorState.numPartitions(); partition++) {
            BucketStreamState streamState = load(aggregatorState, partition);
            if (streamState != null) {
                aggregatorState.set(partition, streamState, false);
            }
        }
        return aggregatorState;
    }

    @Override
    public BucketStreamState load(BucketStreamAggregatorState aggregatorState, int partition) {
        String json = zkClient.readData(pathForState(partition), true);
        if (json == null) {
            return null;
        }
        try {
            JsonNode tree = MAPPER.readTree(json);
            return new BucketStreamState(
                    tree.get("vbucketUUID").asLong(0),
                    tree.get("startSequenceNumber").asLong(0),
                    tree.get("endSequenceNumber").asLong(0),
                    tree.get("snapshotStartSequenceNumber").asLong(0),
                    tree.get("snapshotEndSequenceNumber").asLong(0)
            );
        } catch (IOException ex) {
            LOGGER.warn("Error while decoding state", ex);
            return null;
        }
    }

    private String pathForState(int partition) {
        return Paths.get("/couchbase-kafka-connector", bucket, Integer.toString(partition)).toString();
    }
}
