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

public class AdditionNotification extends Message {

    private static final long serialVersionUID = 1L;

    private NodeAndHash newNode;
    private short[] hashIndexes;
    private NodeAndHash[] hashNodes;

    public AdditionNotification() {
    }

    public AdditionNotification(NodeAndHash addedNode, short[] indexes, NodeAndHash[] nodes) {
        newNode = addedNode;
        hashIndexes = indexes;
        hashNodes = nodes;
    }

    @Override
    public void handleMessage(Connection c, P2PNode handler) {
        completeSource(newNode.getNode(), c);
        for (NodeAndHash resp : hashNodes) {
            completeSource(resp.getNode(), c);
        }
        c.finishConnection();
        handler.forwardAdditionNotification(this);
    }

    public NodeAndHash getNewNode() {
        return newNode;
    }

    public short[] getIndexes() {
        return hashIndexes;
    }

    public NodeAndHash[] getNodes() {
        return hashNodes;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(newNode);
        out.writeObject(hashIndexes);
        for (NodeAndHash node : hashNodes) {
            out.writeObject(node);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        newNode = (NodeAndHash) in.readObject();
        hashIndexes = (short[]) in.readObject();
        hashNodes = new NodeAndHash[hashIndexes.length];
        int i = 0;
        while (in.available() > 0) {
            NodeAndHash n = (NodeAndHash) in.readObject();
            hashNodes[i++] = n;
        }
    }

}
