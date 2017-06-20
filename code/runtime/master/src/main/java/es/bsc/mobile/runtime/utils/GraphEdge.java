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

public class GraphEdge<T, L> implements Comparable<GraphEdge<T, L>> {

    private final T origin;
    private final T target;
    private final L label;

    public GraphEdge(T origin, T target, L label) {
        this.origin = origin;
        this.target = target;
        this.label = label;
    }

    public T getOrigin() {
        return this.origin;
    }

    public T getTarget() {
        return this.target;
    }

    public L getLabel() {
        return label;
    }

    @Override
    public int compareTo(GraphEdge<T, L> o) {
        int dist = origin.hashCode() - o.origin.hashCode();
        if (dist == 0) {
            dist = target.hashCode() - o.target.hashCode();
            if (dist == 0) {
                return label.hashCode() - o.label.hashCode();
            } else {
                return dist;
            }
        } else {
            return dist;
        }

    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof GraphEdge) {
            GraphEdge e = (GraphEdge) o;
            return origin.equals(e.origin) && target.equals(e.target) && label.equals(e.label);
        }
        return false;
    }

    @Override
    public int hashCode() {

        return ((this.origin == null) ? 0 : this.origin.hashCode())
                + ((this.target == null) ? 0 : this.target.hashCode())
                + ((this.label == null) ? 0 : this.label.hashCode());
    }

}
