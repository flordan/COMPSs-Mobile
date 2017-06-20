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
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataExistenceNotification extends Message<RuntimeNode> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LogManager.getLogger("Runtime.DataSharing");
    private static final boolean LOG_ENABLED = LOGGER.isInfoEnabled();

    private HashSet<Node> locations;
    private String data;

    public DataExistenceNotification() {
    }

    public DataExistenceNotification(String data, HashSet<Node> locations) {
        super();
        this.data = data;
        this.locations = locations;
    }

    public String getData() {
        return data;
    }

    public HashSet<Node> getLocations() {
        return locations;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(data);
        out.writeObject(locations);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = in.readUTF();
        locations = (HashSet<Node>) in.readObject();
    }

    @Override
    public void handleMessage(Connection source, RuntimeNode handler) {
        CommunicationManager.receivedNotification(source);
        if (LOG_ENABLED) {
            LOGGER.info("Received data " + data + " existence notification.");
        }
        handler.registerDataExistence(data);
    }

    @Override
    public String toString() {
        return "data " + data + " existence";
    }
}
