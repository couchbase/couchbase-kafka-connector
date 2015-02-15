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
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.cluster.GetClusterConfigRequest;
import com.couchbase.client.core.message.cluster.GetClusterConfigResponse;
import com.couchbase.client.core.message.cluster.OpenBucketRequest;
import com.couchbase.client.core.message.cluster.SeedNodesRequest;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.StreamRequestRequest;
import com.couchbase.client.core.message.dcp.StreamRequestResponse;
import com.couchbase.client.deps.com.lmax.disruptor.EventTranslatorOneArg;
import com.couchbase.client.deps.com.lmax.disruptor.RingBuffer;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func1;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Sergey Avseyev
 */
public class CouchbaseReader {
    private final ClusterFacade core;
    private final RingBuffer<DCPEvent> dcpRingBuffer;
    private final List<String> nodes;
    private final String bucket;
    private final String streamName;
    private final String password;

    private static final EventTranslatorOneArg<DCPEvent, CouchbaseMessage> TRANSLATOR =
            new EventTranslatorOneArg<DCPEvent, CouchbaseMessage>() {
                @Override
                public void translateTo(final DCPEvent event, final long sequence, final CouchbaseMessage message) {
                    event.setMessage(message);
                }
            };

    public CouchbaseReader(final ClusterFacade core, final RingBuffer<DCPEvent> dcpRingBuffer, final List<String> nodes,
                           final String bucket, final String password) {
        this.core = core;
        this.dcpRingBuffer = dcpRingBuffer;
        this.nodes = nodes;
        this.bucket = bucket;
        this.password = password;
        this.streamName = "CouchbaseKafka(" + this.hashCode() + ")";
    }

    public void connect() {
        connect(2, TimeUnit.SECONDS);
    }

    public void connect(final long timeout, final TimeUnit timeUnit) {
        core.send(new SeedNodesRequest(nodes))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();
        core.send(new OpenBucketRequest(bucket, password))
                .timeout(timeout, timeUnit)
                .toBlocking()
                .single();

    }

    public void run() {
        core.send(new OpenConnectionRequest(streamName, bucket))
                .toList()
                .flatMap(new Func1<List<CouchbaseResponse>, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(final List<CouchbaseResponse> couchbaseResponses) {
                        return partitionSize();
                    }
                })
                .flatMap(new Func1<Integer, Observable<DCPRequest>>() {
                    @Override
                    public Observable<DCPRequest> call(final Integer numberOfPartitions) {
                        return requestStreams(numberOfPartitions);
                    }
                })
                .toBlocking()
                .forEach(new Action1<DCPRequest>() {
                    @Override
                    public void call(final DCPRequest dcpRequest) {
                        dcpRingBuffer.tryPublishEvent(TRANSLATOR, dcpRequest);
                    }
                });

    }


    private Observable<Integer> partitionSize() {
        return core
                .<GetClusterConfigResponse>send(new GetClusterConfigRequest())
                .map(new Func1<GetClusterConfigResponse, Integer>() {
                    @Override
                    public Integer call(final GetClusterConfigResponse response) {
                        CouchbaseBucketConfig config = (CouchbaseBucketConfig) response
                                .config().bucketConfig(bucket);
                        return config.numberOfPartitions();
                    }
                });
    }

    private Observable<DCPRequest> requestStreams(final int numberOfPartitions) {
        return Observable.merge(
                Observable.range(0, numberOfPartitions)
                        .flatMap(new Func1<Integer, Observable<StreamRequestResponse>>() {
                            @Override
                            public Observable<StreamRequestResponse> call(final Integer partition) {
                                return core.send(new StreamRequestRequest(partition.shortValue(), bucket));
                            }
                        })
                        .map(new Func1<StreamRequestResponse, Observable<DCPRequest>>() {
                            @Override
                            public Observable<DCPRequest> call(final StreamRequestResponse response) {
                                return response.stream();
                            }
                        })
        );
    }
}
