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
import es.bsc.mobile.types.messages.Message;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class StartP2P extends Message {

    private static final long serialVersionUID = 1L;

    private Node contactNode;
    private boolean responsible;

    public StartP2P() {

    }

    public StartP2P(boolean takeResponsibilities, Node newNode) {
        contactNode = newNode;
        responsible = takeResponsibilities;
    }

    public String getID() {
        return id;
    }

    public Node getContactNode() {
        return contactNode;
    }

    public boolean isResponsible() {
        return responsible;
    }

    @Override
    public void handleMessage(Connection c, P2PNode handler) {
        completeSource(contactNode, c);
        c.finishConnection();
        handler.startP2P(responsible, contactNode);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(contactNode);
        out.writeBoolean(responsible);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        contactNode = (Node) in.readObject();
        responsible = in.readBoolean();
    }

}
