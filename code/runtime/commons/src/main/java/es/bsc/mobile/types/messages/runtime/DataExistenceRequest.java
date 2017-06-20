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
package es.bsc.mobile.types.messages.runtime;

import es.bsc.comm.Connection;
import es.bsc.comm.Node;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.node.RuntimeNode;
import es.bsc.mobile.types.messages.Message;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataExistenceRequest extends Message<RuntimeNode> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.DataSharing");

    private String data;
    private Node source;

    public DataExistenceRequest() {
    }

    public DataExistenceRequest(String data, Node source) {
        super();
        this.data = data;
        this.source = source;
    }

    public String getData() {
        return data;
    }

    public void setSource(Node source) {
        this.source = source;
    }

    public Node getSource() {
        return source;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(data);
        out.writeObject(source);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = in.readUTF();
        source = (Node) in.readObject();
    }

    @Override
    public void handleMessage(Connection source, RuntimeNode handler) {
        CommunicationManager.receivedNotification(source);
        completeSource(this.source, source);
        LOGGER.info("Received data " + data + " existence query from " + this.source + ".");
        handler.handleExistenceRequest(this);
    }

    @Override
    public String toString() {
        return " data " + data + "existence request";
    }
}
