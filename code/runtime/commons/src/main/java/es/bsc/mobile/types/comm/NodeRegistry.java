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
import java.util.TreeMap;

public class NodeRegistry {

    private static final TreeMap<String, Node> REPRESENTATIVE_NODES = new TreeMap<>();

    private NodeRegistry() {

    }

    public static Node getRepresentive(Node node) {
        Node n = REPRESENTATIVE_NODES.get(node.toString());
        if (n == null) {
            n = node;
            REPRESENTATIVE_NODES.put(n.toString(), n);
        }
        return n;
    }

    public static void updateRepresentative(Node oldNode, Node newNode) {
        REPRESENTATIVE_NODES.remove(oldNode.toString());
        REPRESENTATIVE_NODES.put(newNode.toString(), newNode);
    }

}
