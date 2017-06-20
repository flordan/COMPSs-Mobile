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
package es.bsc.mobile.apps.heatsweeper;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Params implements Externalizable {

    public enum Algorithm {

        JACOBI, GAUSS, RED_BLACK
    }

    private int maxIter; // maximum number of iterations
    private int resolution; // spatial resolution
    private Algorithm algorithm; // 0=>Jacobi, 1=>Gauss

    private int visres; // visualization resolution

    private double goal;// Goal temperature

    // Heat Sources
    private HeatSource[] sources; // sources definition

    public Params() {

    }

    public Params(int maxIter, int res, /* Algorithm alg, */ int visRes, double goal, HeatSource[] sources) {
        this.maxIter = maxIter;
        this.resolution = res;
        this.algorithm = /* alg; */ Algorithm.JACOBI;
        this.visres = visRes;
        this.goal = goal;
        this.sources = sources;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public int getResolution() {
        return resolution;
    }

    public int getVisres() {
        return visres;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public int getNumSources() {
        return sources.length;
    }

    public HeatSource[] getSources() {
        return sources;
    }

    public HeatSource getSource(int i) {
        return sources[i];
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Execution Params:\n");
        sb.append("\t * Maximum number of iterations: ").append(maxIter).append("\n");
        sb.append("\t * Resolution: ").append(resolution).append("\n");
        sb.append("\t * Visualization Resolution: ").append(visres).append("\n");
        sb.append("\t * Algorithm: ").append(algorithm).append("\n");
        sb.append("\t * Heat Sources:\n");
        for (int i = 0; i < sources.length; i++) {
            sb.append("\t\t - ").append(sources[i].toString()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        maxIter = in.readInt();
        resolution = in.readInt();
        algorithm = Algorithm.values()[in.readInt()];
        visres = in.readInt();
        goal = in.readDouble();
        sources = (HeatSource[]) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(maxIter);
        out.writeInt(resolution);
        out.writeInt(algorithm.ordinal());
        out.writeInt(visres);
        out.writeDouble(goal);
        out.writeObject(sources);
    }

    public double getGoal() {
        return goal;
    }
}
