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
package es.bsc.mobile.runtime.types.resources.cloudplatforms;

import es.bsc.mobile.runtime.types.resources.Resource;
import es.bsc.mobile.types.comm.Node;

public class RemoteResource extends Resource {

    private static final String SEPARATOR = "<->";
    private final Node node;

    public RemoteResource(Node n, int slots) {
        super(slots);
        this.node = n;
    }

    @Override
    public String getName() {
        return node.toString();
    }

    public Node getNode() {
        return node;
    }

    public static final class NodeCreationException extends Exception {

        public NodeCreationException(String message, Exception cause) {
            super(message, cause);
        }
    }

    public static final class UnknownNodeType extends Exception {

        public UnknownNodeType(String type) {
            super(type + " is not a valid type of worker.");
        }

    }
}
