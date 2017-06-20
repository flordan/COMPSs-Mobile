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

import es.bsc.comm.Node;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class NIONode implements Node, Externalizable {

    private String ip;
    private int port;

    public NIONode() {
    }

    public NIONode(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return this.ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getPort() {
        return this.port;
    }

    // Returns true if the contents are equal
    // Returns false otherwise
    @Override
    public int compareTo(Node no) {
        if (no instanceof NIONode) {
            NIONode n = (NIONode) no;
            if (ip == null) {
                if (n.ip == null) {
                    return n.port - port;
                } else {
                    return 1;
                }
            } else if (n.ip == null) {
                return -1;
            } else {
                int ipDiff = n.ip.compareTo(ip);
                if (ipDiff == 0) {
                    return 0;
                } else {
                    return n.port - port;
                }
            }
        } else {
            return no.getClass().getName().compareTo(this.getClass().getName());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NIONode) {
            NIONode nn = (NIONode) o;
            if (ip == null) {
                return nn.ip == null && nn.port == port;
            } else {
                return nn.ip != null && (nn.ip.compareTo(ip) == 0) && (nn.port == port);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {

        return ((ip == null) ? 0 : ip.hashCode())
                + port;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(port);
        if (ip != null) {
            out.writeUTF(ip);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        port = in.readInt();
        if (in.available() > 0) {
            ip = in.readUTF();
        }
    }

    @Override
    public String toString() {
        return ip + ":" + port;
    }
}
