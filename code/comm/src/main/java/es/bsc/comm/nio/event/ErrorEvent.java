/*
 *  Copyright 2008-2016 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package es.bsc.comm.nio.event;

import es.bsc.comm.CommException;
import es.bsc.comm.EventManager;
import es.bsc.comm.nio.NIOConnection;

public class ErrorEvent extends NIOEvent {

    private final CommException exception;

    public ErrorEvent(NIOConnection nc, CommException e) {
        super(nc);
        exception = e;
    }

    public ErrorEvent(CommException e) {
        super(null);
        exception = e;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public EventType getEventType() {
        return EventType.ERROR;
    }

    @Override
    public void processEventOnConnection(EventManager<?> nem) {
        if (getConnection() != null) {
            getConnection().error(exception);
        } else {
            nem.notifyError(null, null, exception);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        sb.append("Error event");
        if (getConnection() != null) {
            sb.append(" on connection@").append(getConnection().hashCode());
        }
        sb.append(" caused by ").append(exception);

        return sb.toString();
    }
}
