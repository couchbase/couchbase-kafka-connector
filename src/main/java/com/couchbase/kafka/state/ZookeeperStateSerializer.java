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
import com.google.common.base.Joiner;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Sergey Avseyev
 */
public class ZookeeperStateSerializer implements StateSerializer {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperStateSerializer.class);
    private final ZkClient zkClient;
    private final String bucket;
    private final long stateSerializationThreshold;
    private long updatedAt = 0;
    private volatile BucketStreamAggregatorState state = new BucketStreamAggregatorState();

    public ZookeeperStateSerializer(final CouchbaseKafkaEnvironment environment) {
        this.zkClient = new ZkClient(environment.kafkaZookeeperAddress(), 4000, 6000, ZKStringSerializer$.MODULE$);
        this.bucket = environment.couchbaseBucket();
        this.stateSerializationThreshold = environment.couchbaseStateSerializationThreshold();
    }

    @Override
    public void dump(final BucketStreamAggregatorState aggregatorState) {
        for (BucketStreamState streamState : aggregatorState) {
            dump(aggregatorState, streamState.partition());
        }
    }

    @Override
    public void dump(final BucketStreamAggregatorState aggregatorState, final short partition) {
        long now = System.currentTimeMillis();
        if (now - updatedAt > stateSerializationThreshold) {
            final BucketStreamState streamState = aggregatorState.get(partition);
            ObjectNode json = MAPPER.createObjectNode();
            json.put("vbucketUUID", streamState.vbucketUUID());
            json.put("startSequenceNumber", streamState.startSequenceNumber());
            json.put("endSequenceNumber", streamState.endSequenceNumber());
            json.put("snapshotStartSequenceNumber", streamState.snapshotStartSequenceNumber());
            json.put("snapshotEndSequenceNumber", streamState.snapshotEndSequenceNumber());
            zkClient.createPersistent(pathForState(partition), true);
            zkClient.writeData(pathForState(partition), json.toString());
            updatedAt = now;
        }
    }

    @Override
    public BucketStreamAggregatorState load(final BucketStreamAggregatorState aggregatorState) {
        for (BucketStreamState streamState : aggregatorState) {
            BucketStreamState newState = load(aggregatorState, streamState.partition());
            if (newState != null) {
                aggregatorState.put(newState, false);
            }
        }
        return aggregatorState;
    }

    @Override
    public BucketStreamState load(final BucketStreamAggregatorState aggregatorState, short partition) {
        String json = zkClient.readData(pathForState(partition), true);
        if (json == null) {
            return null;
        }
        try {
            JsonNode tree = MAPPER.readTree(json);
            return new BucketStreamState(
                    partition,
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

    private String pathForState(final int partition) {
        return Joiner.on("/").join("/couchbase-kafka-connector", bucket, Integer.toString(partition));
    }
}
