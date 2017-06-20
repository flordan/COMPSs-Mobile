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
package es.bsc.mobile.types.messages;

import es.bsc.comm.Connection;
import es.bsc.comm.Node;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.nio.NIONode;
import es.bsc.mobile.node.P2PNode;
import java.io.Externalizable;
import java.util.UUID;

public abstract class Message<T extends P2PNode> implements Externalizable {

    protected static final long serialVersionUID = 1L;

    public final String id;

    public Message() {
        id = UUID.randomUUID().toString();
    }

    public String getId() {
        return id;
    }

    protected void completeSource(Node node, Connection connection) {
        if (((NIONode) node).getIp() == null) {
            String ip = ((NIONode) ((NIOConnection) connection).getNode()).getIp();
            ((NIONode) node).setIp(ip);
        }
    }

    public abstract void handleMessage(Connection c, T handler);

}
