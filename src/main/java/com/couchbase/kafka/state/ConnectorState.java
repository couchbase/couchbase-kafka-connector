/**
 * Copyright (C) 2016 Couchbase, Inc.
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

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements state of the @{link {@link com.couchbase.kafka.CouchbaseKafkaConnector} instance.
 */
public class ConnectorState implements Iterable<StreamState> {
    private final Map<Short, StreamState> streams;
    private final Subject<StreamStateUpdatedEvent, StreamStateUpdatedEvent> updates =
            PublishSubject.<StreamStateUpdatedEvent>create().toSerialized();

    public ConnectorState() {
        this.streams = new HashMap<Short, StreamState>(1024);
    }

    @Override
    public Iterator<StreamState> iterator() {
        return streams.values().iterator();
    }

    /**
     * Set/update the stream state
     *
     * @param streamState new state for stream
     */
    public void put(StreamState streamState) {
        streams.put(streamState.partition(), streamState);
    }

    /**
     * Returns the stream state.
     *
     * @param partition partition of the stream.
     * @return the state associated or null
     */
    public StreamState get(short partition) {
        return streams.get(partition);
    }

    public short[] partitions() {
        short[] partitions = new short[streams.size()];
        int i = 0;
        for (Short partition : streams.keySet()) {
            partitions[i++] = partition;
        }
        return partitions;
    }

    public ConnectorState clone() {
        ConnectorState newState = new ConnectorState();
        for (Map.Entry<Short, StreamState> entry : streams.entrySet()) {
            newState.streams.put(entry.getKey(), entry.getValue());
        }
        return newState;
    }

    public void update(short partition, long sequenceNumber) {
        StreamState state = streams.get(partition);
        streams.put(partition,
                new StreamState(partition, state.vbucketUUID(), Math.max(state.sequenceNumber(), sequenceNumber)));
        updates.onNext(new StreamStateUpdatedEvent(this, partition));
    }

    public Observable<StreamStateUpdatedEvent> updates() {
        return updates;
    }
}
