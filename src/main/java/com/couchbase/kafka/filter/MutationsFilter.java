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

package com.couchbase.kafka.filter;

import com.couchbase.client.core.message.dcp.MutationMessage;
import com.couchbase.client.core.message.dcp.RemoveMessage;
import com.couchbase.kafka.DCPEvent;

/**
 * The {@link MutationsFilter} allows only mutations to be sent to Kafka.
 *
 * @author Sergey Avseyev
 */
public class MutationsFilter implements Filter {

    /**
     * Returns true if event is mutation.
     *
     * @param dcpEvent event object from Couchbase.
     * @return true if event is mutation.
     */
    public boolean pass(final DCPEvent dcpEvent) {
        return dcpEvent.message() instanceof MutationMessage
                || dcpEvent.message() instanceof RemoveMessage;
    }
}
