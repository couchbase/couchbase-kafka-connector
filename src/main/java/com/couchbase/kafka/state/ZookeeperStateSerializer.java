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

import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import com.couchbase.kafka.CouchbaseKafkaEnvironment;
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

    public ZookeeperStateSerializer(final CouchbaseKafkaEnvironment environment) {
        this.zkClient = new ZkClient(environment.kafkaZookeeperAddress(), 4000, 6000, ZKStringSerializer$.MODULE$);
        this.bucket = environment.couchbaseBucket();
        this.stateSerializationThreshold = environment.couchbaseStateSerializationThreshold();
    }


    @Override
    public void dump(final ConnectorState connectorState) {
        long now = System.currentTimeMillis();
        if (now - updatedAt > stateSerializationThreshold) {
            for (StreamState streamState : connectorState) {
                writeState(streamState);
            }
            updatedAt = now;
        }
    }

    @Override
    public void dump(final ConnectorState connectorState, final short partition) {
        long now = System.currentTimeMillis();
        if (now - updatedAt > stateSerializationThreshold) {
            final StreamState streamState = connectorState.get(partition);
            writeState(streamState);
            updatedAt = now;
        }
    }

    @Override
    public ConnectorState load(final ConnectorState connectorState) {
        for (StreamState streamState : connectorState) {
            StreamState newState = load(connectorState, streamState.partition());
            if (newState != null) {
                connectorState.put(newState);
            }
        }
        return connectorState;
    }

    @Override
    public StreamState load(final ConnectorState connectorState, final short partition) {
        String json = zkClient.readData(pathForState(partition), true);
        if (json == null) {
            return null;
        }
        try {
            JsonNode tree = MAPPER.readTree(json);
            return new StreamState(
                    partition,
                    tree.get("vbucketUUID").asLong(0),
                    tree.get("sequenceNumber").asLong(0)
            );
        } catch (IOException ex) {
            LOGGER.warn("Error while decoding state", ex);
            return null;
        }
    }

    private String pathForState(final short partition) {
        return String.format("/couchbase-kafka-connector2/%s/%d", bucket, partition);
    }

    private void writeState(final StreamState streamState) {
        ObjectNode json = MAPPER.createObjectNode();
        json.put("vbucketUUID", streamState.vbucketUUID());
        json.put("sequenceNumber", streamState.sequenceNumber());
        zkClient.createPersistent(pathForState(streamState.partition()), true);
        zkClient.writeData(pathForState(streamState.partition()), json.toString());
    }
}
