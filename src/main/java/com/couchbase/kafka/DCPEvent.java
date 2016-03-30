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

import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.CouchbaseMessage;
import com.couchbase.client.core.message.dcp.DCPMessage;


/**
 * A pre allocated event which carries a {@link CouchbaseMessage} and associated information.
 *
 * @author Sergey Avseyev
 */

public class DCPEvent {
    /**
     * Current message from the stream.
     */
    private CouchbaseMessage message;

    /**
     * DCP connection instance
     */
    private DCPConnection connection;

    /**
     * Set the new message as a payload for this event.
     *
     * @param message the message to override.
     * @return the {@link DCPEvent} for method chaining.
     */
    public DCPEvent setMessage(final CouchbaseMessage message) {
        this.message = message;
        return this;
    }

    /**
     * Get the message from the payload.
     *
     * @return the actual message.
     */
    public CouchbaseMessage message() {
        return message;
    }


    public void setConnection(DCPConnection connection) {
        this.connection = connection;
    }

    /**
     * Get the associated DCP connection object.
     *
     * @return connection.
     */
    public DCPConnection connection() {
        return connection;
    }

    /**
     * Extract key from the payload.
     *
     * @return the key of message or null.
     */
    public String key() {
        if (message instanceof DCPMessage) {
            return ((DCPMessage) message).key();
        } else {
            return null;
        }
    }
}
