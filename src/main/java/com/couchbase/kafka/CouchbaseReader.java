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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.dcp.BucketStreamAggregator;
import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.core.dcp.BucketStreamStateUpdatedEvent;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.deps.com.lmax.disruptor.EventTranslatorOneArg;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.kafka.state.RunMode;
import com.couchbase.kafka.state.StateSerializer;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private static final EventTranslatorOneArg<DCPEvent, CouchbaseMessage> TRANSLATOR =
            new EventTranslatorOneArg<DCPEvent, CouchbaseMessage>() {
                @Override
                public void translateTo(final DCPEvent event, final long sequence, final CouchbaseMessage message) {
                    event.setMessage(message);
                }
            };
    private final ClusterFacade core;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final List<String> nodes;
    private final String bucket;
    private final String streamName;
    private final String password;
    private final BucketStreamAggregator streamAggregator;
    private final StateSerializer stateSerializer;
    private int numberOfPartitions;


    /**
     * Creates a new {@link CouchbaseReader}.
     *
     * @param core            the core reference.
     * @param environment     the environment object, which carries settings.
     * @param dcpRingBuffer   the buffer where to publish new events.
     * @param stateSerializer the object to serialize the state of DCP streams.
     */
    public CouchbaseReader(final ClusterFacade core, final CouchbaseKafkaEnvironment environment,
                           final RingBuffer<DCPEvent> dcpRingBuffer, final StateSerializer stateSerializer) {
        this(environment.couchbaseNodes(), environment.couchbaseBucket(), environment.couchbasePassword(),
                core, environment, dcpRingBuffer, stateSerializer);
    }

    /**
     * Creates a new {@link CouchbaseReader}.
     *
     * @param couchbaseNodes    list of the Couchbase nodes to override {@link CouchbaseKafkaEnvironment#couchbaseNodes()}
     * @param couchbaseBucket   bucket name to override {@link CouchbaseKafkaEnvironment#couchbaseBucket()}
     * @param couchbasePassword password to override {@link CouchbaseKafkaEnvironment#couchbasePassword()}
     * @param core              the core reference.
     * @param environment       the environment object, which carries settings.
     * @param dcpRingBuffer     the buffer where to publish new events.
     * @param stateSerializer   the object to serialize the state of DCP streams.
     */
    public CouchbaseReader(final List<String> couchbaseNodes, final String couchbaseBucket, final String couchbasePassword,
                           final ClusterFacade core, final CouchbaseKafkaEnvironment environment,
                           final RingBuffer<DCPEvent> dcpRingBuffer, final StateSerializer stateSerializer) {
        this.core = core;
        this.dcpRingBuffer = dcpRingBuffer;
        this.nodes = couchbaseNodes;
        this.bucket = couchbaseBucket;
        this.password = couchbasePassword;
        this.streamAggregator = new BucketStreamAggregator(core, bucket);
        this.stateSerializer = stateSerializer;
        this.streamName = "CouchbaseKafka(" + this.hashCode() + ")";
    }

    /**
     * Performs connection with 2 seconds timeout.
     */
    public void connect() {
        connect(2, TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout  the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    public void connect(final long timeout, final TimeUnit timeUnit) {
        core.send(new SeedNodesRequest(nodes))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        numberOfPartitions = core.<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                })
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
    }

    /**
     * Continue from the state where the stream was left.
     */
    public void run() {
        run(RunMode.LOAD_AND_RESUME);
    }

    /**
     * Run with specified mode.
     *
     * @param mode running mode. See {@link RunMode} for details.
     */
    public void run(final RunMode mode) {
        BucketStreamAggregatorState state = new BucketStreamAggregatorState(streamName);
        for (int i = 0; i < numberOfPartitions; i++) {
            state.put(new BucketStreamState((short) i, 0, 0, 0xffffffff, 0, 0xffffffff));
        }
        run(state, mode);
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     *
     * @param state initial state for the streams
     * @param mode the running mode
     */
    public void run(final BucketStreamAggregatorState state, final RunMode mode) {
        if (mode == RunMode.LOAD_AND_RESUME) {
            stateSerializer.load(state);
        }
        state.updates().subscribe(
                new Action1<BucketStreamStateUpdatedEvent>() {
                    @Override
                    public void call(BucketStreamStateUpdatedEvent event) {
                        if (event.partialUpdate()) {
                            stateSerializer.dump(event.aggregatorState(), event.partitionState().partition());
                        } else {
                            stateSerializer.dump(event.aggregatorState());
                        }
                    }
                });
        streamAggregator.feed(state)
                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage snapshotMarker = (SnapshotMarkerMessage) dcpRequest;
                            final BucketStreamState oldState = state.get(snapshotMarker.partition());
                            state.put(new BucketStreamState(
                                    snapshotMarker.partition(),
                                    oldState.vbucketUUID(),
                                    snapshotMarker.endSequenceNumber(),
                                    oldState.endSequenceNumber(),
                                    snapshotMarker.endSequenceNumber(),
                                    oldState.snapshotEndSequenceNumber()));
                        } else {
                            dcpRingBuffer.tryPublishEvent(TRANSLATOR, dcpRequest);
                        }
                    }
                });
    }
}
