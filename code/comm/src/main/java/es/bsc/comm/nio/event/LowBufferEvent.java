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

import es.bsc.comm.Connection;
import es.bsc.comm.EventManager;
import es.bsc.comm.InternalConnection;
import es.bsc.comm.nio.NIOConnection;

public class LowBufferEvent<T extends Connection & InternalConnection> extends NIOEvent {

    public LowBufferEvent(NIOConnection nc) {
        super(nc);
    }

    @Override
    public EventType getEventType() {
        return EventType.LOW_BUFFER;
    }

    @Override
    public void processEventOnConnection(EventManager<?> ntm) {
        getConnection().lowSendBuffer();
    }

    @Override
    public String toString() {
        return "More To Write Event for connection@" + getConnection().hashCode();
    }

}
