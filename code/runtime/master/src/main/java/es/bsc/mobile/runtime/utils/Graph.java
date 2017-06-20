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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import es.bsc.mobile.runtime.types.exceptions.ElementNotFoundException;

/**
 * The Graph class represents a mathematical graph-theory oriented graph. It
 * contains a set of vertices with their own id and a set of edges that connect
 * a pair of vertices.
 *
 * @param <K> Object type of the vertices Ids
 * @param <T> Object type represented on the vertices
 * @param <L> Object type represented on the labels of the edges
 */
public class Graph<K, T, L> {

    /**
     * relation vertex id --> vertex + edges
     */
    private final Map<K, GraphNode<T, L>> nodes;

    private static final String ERR_ADD_EDGE = "Cannot add the edge: predecessor and/or successor don't exist";

    /**
     * Constructs a new graph
     */
    public Graph() {
        nodes = new TreeMap<>();
    }

    /**
     * Returns the vertex associated with that id
     *
     * @param key id of the vertex
     * @return object on that vertex
     */
    public T get(K key) {
        GraphNode<T, L> n = nodes.get(key);
        if (n == null) {
            return null;
        }
        return (T) n;
    }

    /**
     * Looks for all the vertices who are predecessors of the vetex with key
     * identifier.
     *
     * @param key identifier of the vertex
     * @return a set of elements with some edges pointing to key vertex
     * @throws ElementNotFoundException There is no vertex with key identifier
     */
    public Set<GraphEdge<T, L>> getPredecessors(K key) throws ElementNotFoundException {
        GraphNode<T, L> n = nodes.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getPredecessors();
    }

    /**
     * Looks for all the vertices who are successors of the vertex with key as
     * identifier.
     *
     * @param key identifier of the vertex
     * @return a set of elements with at least an edge which points them from
     * the vertex with identifier key
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public Set<GraphEdge<T, L>> getSuccessors(K key) throws ElementNotFoundException {
        GraphNode<T, L> n = nodes.get(key);
        if (n == null) {
            throw new ElementNotFoundException();
        }
        return n.getSuccessors();
    }

    /**
     * Returns the amount of vertices in the graph
     *
     * @return The amount of vertices in the graph
     */
    public int getSize() {
        return nodes.size();
    }

    /**
     * Looks if the vertex with identifier key has any edge pointing to it
     *
     * @param key identifier of the vertex
     * @return true if the vertex with identifier key has any predecessor
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public boolean hasPredecessors(K key) throws ElementNotFoundException {
        return !this.getPredecessors(key).isEmpty();
    }

    /**
     * Looks if the vertex with identifier key is the source of any edge
     *
     * @param key identifier of the vertex
     * @return true if the vertex with identifier key has any predecessor
     * @throws ElementNotFoundException There is no vertex with identifier key
     */
    public boolean hasSuccessors(K key) throws ElementNotFoundException {
        return !this.getSuccessors(key).isEmpty();
    }

    /**
     * Adds a new vertex to the graph without edges
     *
     * @param key identifier of the new vertex
     * @param element object represented by this vertex
     */
    public void addNode(K key, GraphNode<T, L> element) {
        nodes.put(key, element);
    }

    /**
     * Adds a new edge between two already existing vertices
     *
     * @param sourceKey identifier of the source vertex of the edge
     * @param destKey identifier of the destination vertex of the edge
     * @param label label of the edge
     * @throws ElementNotFoundException Any of the vertices does not exist
     */
    public void addEdge(K sourceKey, K destKey, L label) throws ElementNotFoundException {
        GraphNode<T, L> pred = nodes.get(sourceKey);
        GraphNode<T, L> succ = nodes.get(destKey);
        GraphEdge<T, L> edge = new GraphEdge(pred, succ, label);
        if (pred == null || succ == null) {
            throw new ElementNotFoundException(ERR_ADD_EDGE);
        }

        pred.addSuccessor(edge);
        succ.addPredecessor(edge);
    }

    /**
     * Adds a new edge between two already existing vertices
     *
     * @param origin source vertex of the edge
     * @param target destination vertex of the edge
     * @param label label of the edge
     * @throws ElementNotFoundException Any of the vertices does not exist
     */
    public void addEdge(GraphNode<T, L> origin, GraphNode<T, L> target, L label) throws ElementNotFoundException {
        GraphEdge<T, L> edge = new GraphEdge(origin, target, label);
        if (origin == null || target == null) {
            throw new ElementNotFoundException(ERR_ADD_EDGE);
        }

        origin.addSuccessor(edge);
        target.addPredecessor(edge);
    }

    /**
     * Removes the vertex of the graph with identifier key
     *
     * @param key identifier of the vertex to be removed
     * @return the removed vertex.
     */
    public T removeNode(K key) {
        GraphNode<T, L> n = nodes.remove(key);
        if (n != null) {
            return (T) n;
        }
        return null;
    }

    /**
     * Removes an edge of the graph
     *
     * @param sourceKey source vertex identifier
     * @param destKey destination vertex identifier
     * @param label label of the edge to be removed
     * @throws ElementNotFoundException Any of the vertices does not exist
     */
    public void removeEdge(K sourceKey, K destKey, L label)
            throws ElementNotFoundException {
        GraphNode<T, L> pred = nodes.get(sourceKey);
        GraphNode<T, L> succ = nodes.get(destKey);

        if (pred == null || succ == null) {
            throw new ElementNotFoundException(ERR_ADD_EDGE);
        }
        GraphEdge<T, L> edge = null;
        for (GraphEdge<T, L> ge : pred.getSuccessors()) {
            if (ge.getLabel() == label) {
                edge = ge;
                break;
            }
        }
        if (edge == null) {
            return;
        }
        pred.removeSuccessor(edge);
        succ.removePredecessor(edge);
    }

    /**
     * Removes all the vertices and edges in the graph
     */
    public void clear() {
        nodes.clear();
    }

}
