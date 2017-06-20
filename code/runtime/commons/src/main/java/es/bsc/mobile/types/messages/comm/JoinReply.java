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
import es.bsc.comm.Node;
import es.bsc.mobile.node.P2PNode;
import es.bsc.mobile.types.comm.NodeAndHash;
import es.bsc.mobile.types.messages.Message;
import es.bsc.mobile.utils.DistributedHashTable.Register;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

public class JoinReply extends Message {

    private static final long serialVersionUID = 1L;

    private NodeAndHash successor;
    private LinkedList<NodeAndHash> representeds;
    private LinkedList<Register> registers;

    public JoinReply() {
    }

    public JoinReply(NodeAndHash suc, LinkedList<NodeAndHash> rep, LinkedList<Register> regs) {
        successor = suc;
        representeds = rep;
        registers = regs;
    }

    public NodeAndHash getSuccessor() {
        return successor;
    }

    public LinkedList<NodeAndHash> getRepresenteds() {
        return representeds;
    }

    public LinkedList<Register> getRegisters() {
        return registers;
    }

    @Override
    public void handleMessage(Connection c, P2PNode handler) {
        completeSource(successor.getNode(), c);
        for (Register r : registers) {
            for (Node n : r.getAllNodes()) {
                completeSource(n, c);
            }
        }
        c.finishConnection();
        handler.joinReplied(this);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(successor);
        out.writeInt(representeds.size());
        for (NodeAndHash rep : representeds) {
            out.writeObject(rep);
        }
        for (Register r : registers) {
            out.writeObject(r);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        successor = (NodeAndHash) in.readObject();
        int size = in.readInt();
        representeds = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            representeds.add((NodeAndHash) in.readObject());
        }
        registers = new LinkedList<>();
        if (in.available() > 0) {
            registers.add((Register) in.readObject());
        }
    }

}
