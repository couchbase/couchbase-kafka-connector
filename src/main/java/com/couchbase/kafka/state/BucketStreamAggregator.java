/*
 * Copyright (c) 2016 Couchbase, Inc.
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

package com.couchbase.kafka.state;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience;
import com.couchbase.client.core.annotations.InterfaceStability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.dcp.BucketStreamAggregatorState;
import com.couchbase.client.core.dcp.BucketStreamState;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.dcp.*;
import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides a higher level abstraction over a DCP stream.
 *
 * The bucket is expected to be opened already when the {@link #feed()} method is called.
 *
 * @author Michael Nitschinger
 * @since 1.2.0
 */
@InterfaceStability.Experimental
@InterfaceAudience.Public
public class BucketStreamAggregator {
    public static String DEFAULT_CONNECTION_NAME = "kafkaConnector";

    private final ClusterFacade core;
    private final String bucket;
    private final String name;
    private final AtomicReference<DCPConnection> connection = new AtomicReference<DCPConnection>();

    public BucketStreamAggregator(final ClusterFacade core, final String bucket) {
        this(DEFAULT_CONNECTION_NAME, core, bucket);
    }

    /**
     * Create BucketStreamAggregator instance
     *
     * @param name   name for DCP connection
     * @param core   core
     * @param bucket bucket name
     */
    public BucketStreamAggregator(final String name, final ClusterFacade core, final String bucket) {
        this.core = core;
        this.bucket = bucket;
        this.name = name;
    }

    public String name() {
        return name;
    }

    /**
     * Opens a DCP stream with default name and returns the feed of changes from beginning.
     *
     * @return the feed with {@link DCPRequest}s.
     */
    public Observable<DCPRequest> feed() {
        final BucketStreamAggregatorState state = new BucketStreamAggregatorState();
        int numPartitions = partitionSize().toBlocking().first();
        for (short partition = 0; partition < numPartitions; partition++) {
            state.put(new BucketStreamState(partition, 0, 0, 0xffffffff, 0, 0xffffffff));
        }
        return feed(state);
    }

    /**
     * Opens a DCP stream and returns the feed of changes.
     *
     * @param aggregatorState state object
     * @return the feed with {@link DCPRequest}s.
     */
    public Observable<DCPRequest> feed(final BucketStreamAggregatorState aggregatorState) {
        return open()
                .flatMap(new Func1<DCPConnection, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(final DCPConnection response) {
                        return Observable
                                .from(aggregatorState)
                                .flatMap(new Func1<BucketStreamState, Observable<StreamRequestResponse>>() {
                                    @Override
                                    public Observable<StreamRequestResponse> call(final BucketStreamState feed) {
                                        Observable<StreamRequestResponse> res =
                                                core.send(new StreamRequestRequest(feed.partition(), feed.vbucketUUID(),
                                                        feed.startSequenceNumber(), feed.endSequenceNumber(),
                                                        feed.snapshotStartSequenceNumber(), feed.snapshotEndSequenceNumber(),
                                                        bucket));
                                        return res.flatMap(new Func1<StreamRequestResponse, Observable<StreamRequestResponse>>() {
                                            @Override
                                            public Observable<StreamRequestResponse> call(StreamRequestResponse response) {
                                                long rollbackSequenceNumber;
                                                switch (response.status()) {
                                                    case RANGE_ERROR:
                                                        rollbackSequenceNumber = 0;
                                                        break;
                                                    case ROLLBACK:
                                                        rollbackSequenceNumber = response.rollbackToSequenceNumber();
                                                        break;
                                                    default:
                                                        return Observable.just(response);
                                                }
                                                return core.send(new StreamRequestRequest(feed.partition(), feed.vbucketUUID(),
                                                        rollbackSequenceNumber, feed.endSequenceNumber(),
                                                        rollbackSequenceNumber, feed.snapshotEndSequenceNumber(),
                                                        bucket));
                                            }
                                        });
                                    }
                                })
                                .toList()
                                .flatMap(new Func1<List<StreamRequestResponse>, Observable<DCPRequest>>() {
                                    @Override
                                    public Observable<DCPRequest> call(List<StreamRequestResponse> streamRequestResponses) {
                                        return connection.get().subject();
                                    }
                                });
                    }
                });
    }

    /**
     * Retrieve current state of the partitions.
     *
     * It has all sequence and snapshot numbers set to the last known sequence number.
     *
     * @return state object
     */
    public Observable<BucketStreamAggregatorState> getCurrentState() {
        return open().flatMap(new Func1<DCPConnection, Observable<BucketStreamAggregatorState>>() {
            @Override
            public Observable<BucketStreamAggregatorState> call(DCPConnection dcpConnection) {
                return partitionSize().flatMap(new Func1<Integer, Observable<BucketStreamAggregatorState>>() {
                    @Override
                    public Observable<BucketStreamAggregatorState> call(final Integer numPartitions) {
                        return Observable.range(0, numPartitions).flatMap(new Func1<Integer, Observable<GetFailoverLogResponse>>() {
                            @Override
                            public Observable<GetFailoverLogResponse> call(final Integer partition) {
                                return core.send(new GetFailoverLogRequest(partition.shortValue(), bucket));
                            }
                        }).flatMap(new Func1<GetFailoverLogResponse, Observable<BucketStreamState>>() {
                            @Override
                            public Observable<BucketStreamState> call(final GetFailoverLogResponse failoverLogsResponse) {
                                final FailoverLogEntry entry = failoverLogsResponse.failoverLog().get(0);
                                return core.<GetLastCheckpointResponse>send(new GetLastCheckpointRequest(failoverLogsResponse.partition(), bucket))
                                        .map(new Func1<GetLastCheckpointResponse, BucketStreamState>() {
                                            @Override
                                            public BucketStreamState call(GetLastCheckpointResponse lastCheckpointResponse) {
                                                return new BucketStreamState(
                                                        failoverLogsResponse.partition(),
                                                        entry.vbucketUUID(),
                                                        lastCheckpointResponse.sequenceNumber());
                                            }
                                        });
                            }
                        }).collect(new Func0<BucketStreamAggregatorState>() {
                            @Override
                            public BucketStreamAggregatorState call() {
                                return new BucketStreamAggregatorState();
                            }
                        }, new Action2<BucketStreamAggregatorState, BucketStreamState>() {
                            @Override
                            public void call(BucketStreamAggregatorState aggregatorState, BucketStreamState streamState) {
                                aggregatorState.put(streamState);
                            }
                        });
                    }
                });
            }
        });
    }

    private Observable<DCPConnection> open() {
        if (connection.get() == null) {
            return core.<OpenConnectionResponse>send(new OpenConnectionRequest(name, bucket))
                    .flatMap(new Func1<OpenConnectionResponse, Observable<DCPConnection>>() {
                        @Override
                        public Observable<DCPConnection> call(final OpenConnectionResponse response) {
                            connection.compareAndSet(null, response.connection());
                            return Observable.just(connection.get());
                        }
                    });
        }
        return Observable.just(connection.get());
    }

    /**
     * Helper method to fetch the number of partitions.
     *
     * @return the number of partitions.
     */
    private Observable<Integer> partitionSize() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response.config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                });
    }
}
