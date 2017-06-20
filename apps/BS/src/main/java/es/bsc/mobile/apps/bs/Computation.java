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
package es.bsc.mobile.apps.bs;

import es.bsc.mobile.annotations.Orchestration;
import java.lang.reflect.Array;

@Orchestration
public class Computation {

    public static void main(String args[]) throws Exception {
        Params p = new Params();
        Computation.start(p);
    }

    public static void start(Params p) throws Exception {

        int in_size = (p.getInRows() + 1) * (p.getInCols() + 1);

        float[] in = new float[in_size * 3];

        int nTasksI = (p.getOutRows() + p.getTileSize() - 1) / p.getTileSize();
        int nTasksJ = (p.getOutCols() + p.getTileSize() - 1) / p.getTileSize();
        System.out.println("Covering surface in a " + nTasksI + "*" + nTasksJ + " tiles");

        Utils.loadData(in, p);
        float[][][] out;
        System.out.println("--- WARM UP STARTS ------");
        for (int rep = 0; rep < p.getnWarmup(); ++rep) {
            out = processSurface(in, p.getInRows(), p.getInCols(), p.getOutRows(), p.getOutCols(), nTasksI, nTasksJ, p.getTileSize());
            for (float[][] out1 : out) {
                for (float[] out11 : out1) {
                    int l = Array.getLength(out11);
                    System.out.print("X");
                }
                System.out.println();
            }
            System.out.println("----------------");
        }

        System.out.println("--- WARM UP ENDS ------");

        long time = System.currentTimeMillis();
        out = processSurface(in, p.getInRows(), p.getInCols(), p.getOutRows(), p.getOutCols(), nTasksI, nTasksJ, p.getTileSize());

        for (float[][] out1 : out) {
            for (float[] out11 : out1) {
                int l = Array.getLength(out11);
            }
        }
        long elapsedTime = System.currentTimeMillis() - time;
        System.out.println("Elapsed time on execution " + elapsedTime);
        verify(in, out, p.getInRows(), p.getInCols(), p.getOutRows(), p.getOutCols(), p.getTileSize());

    }

    private static float[][][] processSurface(float[] in, int inRows, int inCols, int outRows, int outCols, int nTasksI, int nTasksJ, int tileSize) {
        float[][][] out = new float[nTasksI][nTasksJ][];
        for (int i = 0; i < nTasksI; i++) {
            for (int j = 0; j < nTasksJ; j++) {
                out[i][j] = Operations.processTile(in, inRows, inCols, outRows, outCols, i, j, tileSize);
            }
        }
        return out;
    }

    private static void verify(float[] in, float[][][] out, int inRows, int inCols, int outRows, int outCols, int tileSize) {
        float[] gold = new float[outRows * outCols * 3];
        Utils.BezierCPU(in, gold, inRows, inCols, outRows, outRows);
        compareOutputs(out, gold, outRows, outRows, tileSize);
    }

    private static int compareOutputs(float[][][] outp, float[] outpCPU, int RESOLUTIONI, int RESOLUTIONJ, int tileSize) {
        double sum_delta2, sum_ref2, L1norm2;
        sum_delta2 = 0;
        sum_ref2 = 0;
        L1norm2 = 0;

        int nTasksI = (RESOLUTIONI + tileSize - 1) / tileSize;
        int nTasksJ = (RESOLUTIONJ + tileSize - 1) / tileSize;

        for (int tileI = 0; tileI < nTasksI; tileI++) {
            for (int tileJ = 0; tileJ < nTasksJ; tileJ++) {
                double[] values = Utils.validateTyle(outp[tileI][tileJ], outpCPU, RESOLUTIONI, RESOLUTIONJ, tileSize, tileI, tileJ);
                sum_delta2 += values[0];
                sum_ref2 += values[1];
            }
        }
        
        L1norm2 = (double) (sum_delta2 / sum_ref2);
        System.out.println("TEST " + ((L1norm2 < 1e-6) ? "PASSED" : "FAILED"));
        return (L1norm2 < 1e-6) ? 0 : 1;
    }
}
