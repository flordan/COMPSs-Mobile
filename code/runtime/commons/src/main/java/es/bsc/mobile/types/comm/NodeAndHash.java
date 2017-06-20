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
package es.bsc.mobile.types.comm;

import es.bsc.comm.Node;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;

public class NodeAndHash implements Externalizable {

    private static final MessageDigest CRYPT_ALGORITHM;

    private Node node;
    private Short hash;

    static {
        MessageDigest hashAlgorithm;
        try {
            hashAlgorithm = MessageDigest.getInstance("SHA-1");
        } catch (Exception e) {
            hashAlgorithm = null;
        }
        CRYPT_ALGORITHM = hashAlgorithm;
    }

    public NodeAndHash() {
    }

    public NodeAndHash(Node n) {
        node = n;
    }

    public NodeAndHash(Node n, String id) {
        node = n;
        hash = getHash(id);
    }

    public Short getHash() {
        return hash;
    }

    public Node getNode() {
        return node;
    }

    public void setHash(Short hash) {
        this.hash = hash;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public static final short getHash(String s) {
        try {
            byte[] b = CRYPT_ALGORITHM.digest(s.getBytes("UTF-8"));
            return (short) (b[0] * 256 + b[1]);
        } catch (UnsupportedEncodingException e) {
            return 0;
        }
    }

    public static final int getHashSize() {
        return Short.SIZE;
    }

    public static boolean isCloserPreviousThan(NodeAndHash a, NodeAndHash b, NodeAndHash ref) {
        return isCloserPreviousThan(a.getHash(), b.getHash(), ref.getHash());
    }

    public static boolean isCloserPreviousThan(NodeAndHash a, NodeAndHash b, Short ref) {
        return isCloserPreviousThan(a.getHash(), b.getHash(), ref);
    }

    public static boolean isCloserPreviousThan(Short a, Short b, Short ref) {
        if (a.equals(b)) {
            return false;
        } else if (a > b) {
            return (ref >= a || ref < b);
        } else {
            return (ref >= a && ref < b);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(node);
        out.writeShort(hash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        node = (Node) in.readObject();
        hash = in.readShort();
    }
}
