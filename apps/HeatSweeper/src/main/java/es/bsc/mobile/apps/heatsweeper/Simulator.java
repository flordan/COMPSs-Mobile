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

public class Simulator {

    private static void initializeValues(HeatSource[] sources, double[][] values) {
        for (HeatSource sc : sources) {
            double tempSize = 1 / sc.getSize() * sc.getTemp();
            for (int j = 0; j < values[0].length; j++) {
                double viewRatio = (double) (j) / (double) (values[0].length - 1);
                // TOP
                double dist = Math.sqrt(Math.pow(viewRatio - sc.getPosX(), 2) + Math.pow(sc.getPosY(), 2));

                if (dist <= sc.getSize()) {
                    values[0][j] += (sc.getSize() - dist) * tempSize;
                }
                // BOTTOM
                dist = Math.sqrt(Math.pow(viewRatio - sc.getPosX(), 2) + Math.pow(1 - sc.getPosY(), 2));

                if (dist <= sc.getSize()) {
                    values[values[0].length - 1][j] += (sc.getSize() - dist) * tempSize;
                }
                // LEFT
                dist = Math.sqrt(Math.pow(sc.getPosX(), 2) + Math.pow(viewRatio - sc.getPosY(), 2));

                if (dist <= sc.getSize()) {
                    values[j][0] += (sc.getSize() - dist) * tempSize;
                }
                // RIGHT
                dist = Math.sqrt(Math.pow(1 - sc.getPosX(), 2) + Math.pow(viewRatio - sc.getPosY(), 2));

                if (dist <= sc.getSize()) {
                    values[j][values.length - 1] += (sc.getSize() - dist) * tempSize;
                }
            }
        }
    }

    public static Report solve(Params params) {
        Long time = System.currentTimeMillis();
        int np = params.getResolution() + 2;
        int maxIter = params.getMaxIter();
        double[][] values = new double[np][np];
        double goal = params.getGoal();
        System.out.println("Solving " + maxIter + " itertations simulation for heat Sources:");
        for (HeatSource hs : params.getSources()) {
            System.out.println(hs.toString());
        }
        initializeValues(params.getSources(), values);

        double[][] supportMatrix = new double[np][np];
        supportMatrix[0] = values[0];
        supportMatrix[np - 1] = values[np - 1];
        for (int i = 1; i < np - 1; i++) {
            supportMatrix[i][0] = values[i][0];
            supportMatrix[i][np - 1] = values[i][np - 1];
        }
        int it = 0;
        double distance = 0.0d;
        for (it = 0; it < maxIter; it++) {
            double residual = 0.0d;
            distance = 0.0d;
            for (int i = 1; i < np - 1; i++) {
                for (int j = 1; j < np - 1; j++) {
                    supportMatrix[i][j] = 0.25
                            * (values[i - 1][j] + values[i + 1][j] + values[i][j - 1] + values[i][j + 1]);
                    double diff = supportMatrix[i][j] - values[i][j];
                    double diff2 = supportMatrix[i][j] - goal;
                    residual += diff * diff;
                    distance += diff2 * diff2;
                }
            }
            double[][] swap = values;
            values = supportMatrix;
            supportMatrix = swap;
            if (residual < 0.00005d || distance < 0.00005d) {
                continue;
            }
        }
        System.out.println("Ha tardat " + (System.currentTimeMillis() - time));
        return new Report(params.getSources(), it, distance < 0.0005d);
    }

    private void printMatrix(double[][] values) {
        for (int i = 0; i < values.length; i++) {
            for (int j = 0; j < values[i].length; j++) {
                System.out.print(" " + values[i][j]);
            }
            System.out.println();
        }
    }
}
