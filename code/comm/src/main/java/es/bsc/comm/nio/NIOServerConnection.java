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
package es.bsc.comm.nio;

import es.bsc.comm.stage.Reception;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NIOServerConnection extends NIOConnection {

    private boolean uselessConnection;

    public NIOServerConnection(NIOEventManager ntm, SocketChannel sc, NIONode n) {
        super(ntm, sc, n);
        uselessConnection = true;
    }

    @Override
    public void receivedPacket(ByteBuffer buffer) {
        if (uselessConnection) {
            uselessConnection = false;
            currentStage = new Reception(false);
            startCurrentTransfer();
        }
        super.receivedPacket(buffer);
    }

    @Override
    public void closedChannel() {
        if (uselessConnection) {
            LOGGER.debug("Closing useless server connection " + this);
            unregisterChannel();
        } else {
            super.closedChannel();
        }
    }
}
