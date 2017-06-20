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
package es.bsc.mobile.utils;

import es.bsc.mobile.types.comm.NodeAndHash;
import es.bsc.mobile.types.comm.NodeRegistry;

public class Fingers {

    private final short[] hashIndex;
    private final NodeAndHash[] hashNodes;

    public Fingers(short[] indexes, NodeAndHash[] nodes) {
        hashIndex = indexes;
        hashNodes = nodes;
    }

    public Fingers(NodeAndHash me, int fingersCount) {
        hashIndex = new short[fingersCount];
        hashNodes = new NodeAndHash[fingersCount];
        hashIndex[0] = me.getHash();
        hashNodes[0] = me;
        int diff = 1;
        for (int i = 1; i < fingersCount; i++) {
            hashIndex[i] = (short) (hashIndex[0] + diff);
            diff *= 2;
            hashNodes[i] = me;
        }
    }

    public short[] getIndexes() {
        return hashIndex;
    }

    public NodeAndHash[] getResponsibles() {
        return hashNodes;
    }

    public NodeAndHash getResponsibleFor(short key) {
        int pos = hashIndex.length - 1;
        if (NodeAndHash.isCloserPreviousThan(hashIndex[pos], hashIndex[0], key)) {
            return hashNodes[pos];
        }
        pos--;
        for (; pos >= 0; pos--) {
            if (NodeAndHash.isCloserPreviousThan(hashIndex[pos], hashIndex[pos + 1], key)) {
                return hashNodes[pos];
            }
        }
        return null;
    }

    public void setAllResponsible(NodeAndHash newNode) {
        for (int i = 0; i < hashIndex.length; i++) {
            hashNodes[i] = newNode;
        }
    }

    public boolean updateResponsibles(NodeAndHash newNode) {
        boolean updated = false;
        for (int i = 0; i < hashIndex.length; i++) {
            NodeAndHash oldNode = hashNodes[i];
            if (NodeAndHash.isCloserPreviousThan(newNode.getHash(), oldNode.getHash(), hashIndex[i])) {
                hashNodes[i] = newNode;
                updated = true;
            }
        }
        return updated;
    }

    public void replace(Fingers newFingers) {
        short[] newIndexes = newFingers.getIndexes();
        NodeAndHash[] newNodes = newFingers.getResponsibles();
        for (int i = 0; i < hashNodes.length; i++) {
            hashIndex[i] = newIndexes[i];
            hashNodes[i] = newNodes[i];
            newNodes[i].setNode(NodeRegistry.getRepresentive(newNodes[i].getNode()));
        }
    }

    public String dump() {
        StringBuilder sb = new StringBuilder("Fingers: \n");
        sb.append("HashCode -> Initial Hash\tNode\n");
        for (int i = 0; i < hashNodes.length; i++) {
            sb.append(hashIndex[i])
                    .append("\t -> ").append(hashNodes[i].getHash());
            if (hashNodes[i].getHash().toString().length() < 4) {
                sb.append("\t");
            }
            sb.append("\t\t").append(hashNodes[i].getNode())
                    .append("\n");
        }
        return sb.toString();
    }

    public void setResponsibles(NodeAndHash[] newNodes) {
        for (int i = 0; i < hashNodes.length; i++) {
            hashNodes[i] = newNodes[i];
        }
    }

}
