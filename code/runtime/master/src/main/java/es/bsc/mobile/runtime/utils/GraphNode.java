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
package es.bsc.mobile.runtime.utils;

import java.util.TreeSet;

public class GraphNode<E, L> {

    // Node fields
    /**
     * set of object with one edge pointing to it
     */
    private final TreeSet<GraphEdge<E, L>> predecessors;
    /**
     * set of objects with
     */
    private final TreeSet<GraphEdge<E, L>> successors;

    /**
     * Constructs a new Node
     *
     */
    public GraphNode() {
        this.predecessors = new TreeSet<>();
        this.successors = new TreeSet<>();
    }

    /**
     * Gets all the objects who are predecessors of this one
     *
     * @return set of object with at least 1 edge pointing to this node
     */
    public TreeSet<GraphEdge<E, L>> getPredecessors() {
        return predecessors;
    }

    /**
     * Gets all the objects who are successors of this one
     *
     * @return set of object with at least 1 edge starting on this node
     */
    public TreeSet<GraphEdge<E, L>> getSuccessors() {
        return successors;
    }

    /**
     * Adds a new Predecessor to the node
     *
     * @param pred edge which precedes this one
     */
    public void addPredecessor(GraphEdge<E, L> pred) {
        predecessors.add(pred);
    }

    /**
     * Adds a new successor to the node
     *
     * @param succ edge which successes this one
     */
    public void addSuccessor(GraphEdge<E, L> succ) {
        successors.add(succ);
    }

    /**
     * Removes a predecessor of the node
     *
     * @param pred edge which won't precedes this one no more
     */
    public void removePredecessor(GraphEdge<E, L> pred) {
        predecessors.remove(pred);
    }

    /**
     * Removes a successor of the node
     *
     * @param succ edge which won't success this one no more
     */
    public void removeSuccessor(GraphEdge<E, L> succ) {
        successors.remove(succ);
    }
}
