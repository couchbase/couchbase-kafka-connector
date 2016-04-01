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
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.logging.CouchbaseLogger;
import com.couchbase.client.core.logging.CouchbaseLoggerFactory;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.OpenBucketResponse;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.cluster.SeedNodesResponse;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.client.core.message.dcp.SnapshotMarkerMessage;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.deps.com.lmax.disruptor.EventTranslatorTwoArg;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import com.couchbase.kafka.state.ConnectorState;
import com.couchbase.kafka.state.StateSerializer;
import com.couchbase.kafka.state.StreamState;
import com.couchbase.kafka.state.StreamStateUpdatedEvent;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private static final CouchbaseLogger LOGGER = CouchbaseLoggerFactory.getInstance(CouchbaseReader.class);

    private static final EventTranslatorTwoArg<DCPEvent, DCPConnection, CouchbaseMessage> TRANSLATOR =
            new EventTranslatorTwoArg<DCPEvent, DCPConnection, CouchbaseMessage>() {
                @Override
                public void translateTo(final DCPEvent event, final long sequence,
                                        final DCPConnection connection, final CouchbaseMessage message) {
                    event.setMessage(message);
                    event.setConnection(connection);
                }
            };

    private final ClusterFacade core;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final List<String> nodes;
    private final String bucket;
    private final String password;
    private final StateSerializer stateSerializer;
    private final String connectionName;
    private final CouchbaseKafkaEnvironment environment;
    private DCPConnection connection;

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
        this.stateSerializer = stateSerializer;
        this.connectionName = "CouchbaseKafka(" + this.hashCode() + ")";
        this.environment = environment;
    }

    /**
     * Performs connection with default timeout.
     */
    public void connect() {
        connect(environment.connectTimeout(), TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout  the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    public void connect(final long timeout, final TimeUnit timeUnit) {
        OpenConnectionResponse response = core
                .<SeedNodesResponse>send(new SeedNodesRequest(nodes))
                .flatMap(new Func1<SeedNodesResponse, Observable<OpenBucketResponse>>() {
                    @Override
                    public Observable<OpenBucketResponse> call(SeedNodesResponse response) {
                        return core.send(new OpenBucketRequest(bucket, password));
                    }
                })
                .flatMap(new Func1<OpenBucketResponse, Observable<OpenConnectionResponse>>() {
                    @Override
                    public Observable<OpenConnectionResponse> call(OpenBucketResponse response) {
                        return core.send(new OpenConnectionRequest(connectionName, bucket));
                    }
                })
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        this.connection = response.connection();
    }


    /**
     * Returns current state of the cluster.
     *
     * @return and object, which contains current sequence number for each partition on the cluster.
     */
    public ConnectorState currentState() {
        return connection.getCurrentState()
                .collect(new Func0<ConnectorState>() {
                    @Override
                    public ConnectorState call() {
                        return new ConnectorState();
                    }
                }, new Action2<ConnectorState, MutationToken>() {
                    @Override
                    public void call(ConnectorState connectorState, MutationToken token) {
                        connectorState.put(new StreamState(token));
                    }
                })
                .toBlocking().single();
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     *
     * @param fromState initial state for the streams
     * @param toState   target state for the streams
     */
    public void run(final ConnectorState fromState, final ConnectorState toState) {
        if (!Arrays.equals(fromState.partitions(), toState.partitions())) {
            throw new IllegalArgumentException("partitions in FROM state do not match partitions in TO state");
        }

        final ConnectorState connectorState = fromState.clone();
        connectorState.updates().subscribe(
                new Action1<StreamStateUpdatedEvent>() {
                    @Override
                    public void call(StreamStateUpdatedEvent event) {
                        stateSerializer.dump(event.connectorState(), event.partition());
                    }
                });

        Observable.from(fromState)
                .flatMap(new Func1<StreamState, Observable<ResponseStatus>>() {
                    @Override
                    public Observable<ResponseStatus> call(StreamState begin) {
                        StreamState end = toState.get(begin.partition());
                        return connection.addStream(begin.partition(), begin.vbucketUUID(),
                                begin.sequenceNumber(), end.sequenceNumber(),
                                begin.sequenceNumber(), end.sequenceNumber());
                    }
                })
                .toList()
                .flatMap(new Func1<List<ResponseStatus>, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(List<ResponseStatus> statuses) {
                        return connection.subject();
                    }
                })
                .onBackpressureBuffer(environment.kafkaEventBufferSize())
                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        if (dcpRequest instanceof SnapshotMarkerMessage) {
                            SnapshotMarkerMessage snapshotMarker = (SnapshotMarkerMessage) dcpRequest;
                            connectorState.update(snapshotMarker.partition(), snapshotMarker.endSequenceNumber());
                        } else if (dcpRequest instanceof RemoveMessage) {
                            RemoveMessage msg = (RemoveMessage) dcpRequest;
                            connectorState.update(msg.partition(), msg.bySequenceNumber());
                        } else if (dcpRequest instanceof MutationMessage) {
                            MutationMessage msg = (MutationMessage) dcpRequest;
                            connectorState.update(msg.partition(), msg.bySequenceNumber());
                        }
                        dcpRingBuffer.publishEvent(TRANSLATOR, connection, dcpRequest);
                    }
                });
    }
}
