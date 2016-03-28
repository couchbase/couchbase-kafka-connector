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

import com.couchbase.kafka.CouchbaseKafkaEnvironment;

/**
 * Represents serialized which does not persist the state.
 *
 * @author Sergey Avseyev
 */
public class NullStateSerializer implements StateSerializer {

    public NullStateSerializer(final CouchbaseKafkaEnvironment environment) {
    }

    @Override
    public void dump(ConnectorState connectorState) {
    }

    @Override
    public void dump(ConnectorState connectorState, short partition) {
    }

    @Override
    public ConnectorState load(ConnectorState connectorState) {
        return new ConnectorState();
    }

    @Override
    public StreamState load(ConnectorState connectorState, short partition) {
        return new StreamState(partition, 0, 0);
    }
}
