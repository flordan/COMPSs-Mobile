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

import es.bsc.comm.EventManager;
import es.bsc.comm.nio.NIOConnection;
import java.nio.ByteBuffer;

public class PacketEntry extends NIOEvent {

    private final ByteBuffer buffer;

    public PacketEntry(NIOConnection nc, ByteBuffer buff) {
        super(nc);
        buffer = buff;
    }

    @Override
    public EventType getEventType() {
        return EventType.PACKET_ENTRY;
    }

    @Override
    public void processEventOnConnection(EventManager<?> ntm) {
        getConnection().receivedPacket(buffer);
    }

    @Override
    public String toString() {
        return "Package received through  connection@" + getConnection().hashCode();
    }
}
