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
package es.bsc.mobile.types.messages.comm;

import es.bsc.comm.Connection;
import es.bsc.mobile.node.P2PNode;
import es.bsc.mobile.types.comm.NodeAndHash;
import es.bsc.mobile.types.messages.Message;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class JoinRequest extends Message {

    private static final long serialVersionUID = 1L;

    private NodeAndHash node;
    private boolean responsible;

    public JoinRequest() {

    }

    public JoinRequest(NodeAndHash newNode, boolean takeResponsibilities) {
        node = newNode;
        responsible = takeResponsibilities;
    }

    public NodeAndHash getNodeAndHash() {
        return node;
    }

    public boolean isResponsible() {
        return responsible;
    }

    @Override
    public void handleMessage(Connection c, P2PNode handler) {
        completeSource(node.getNode(), c);
        c.finishConnection();
        handler.joinP2P(this);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(node);
        out.writeBoolean(responsible);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        node = (NodeAndHash) in.readObject();
        responsible = in.readBoolean();
    }

}
