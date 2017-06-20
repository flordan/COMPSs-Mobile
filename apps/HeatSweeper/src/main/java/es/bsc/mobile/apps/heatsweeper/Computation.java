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

import es.bsc.mobile.annotations.Orchestration;

@Orchestration
public class Computation {

    static int solve;
    static int merge = 0;

    public static String start(int maxSource, float variation, int iterations) {
        solve = 0;
        Report totalBest = null;
        for (int numSrcs = 1; numSrcs <= maxSource; numSrcs++) {
            Report result = checkSources(0, new HeatSource[numSrcs], variation, iterations);
            if (totalBest == null) {
                totalBest = result;
            } else if (result != null) {
                totalBest = Report.getBest(totalBest, result);
            }
        }
        return totalBest.toString();
    }

    private static Report checkSources(int sourceId, HeatSource[] hSrcs, float variation, int iterations) {
        if (sourceId == hSrcs.length) {
            Params p = new Params(iterations, 512, /* JACOBI, */ 256, 0.5, hSrcs);
            solve++;
            Report r = Simulator.solve(p);
            return r;
        } else {
            float firstX;
            float firstY;
            if (sourceId == 0) {
                firstX = 0.0f;
                firstY = 0.0f;
            } else {
                firstX = hSrcs[sourceId - 1].getPosX();
                firstY = hSrcs[sourceId - 1].getPosY() + variation;
            }
            int size = getMissing(variation, firstX, firstY);
            Report[] results = new Report[size];
            int rIdx = 0;
            for (float i = firstX; i <= 1.0; i += variation) {
                for (float j = firstY; j <= 1.0; j += variation) {
                    if (getMissing(variation, i, j) > 0) {
                        hSrcs[sourceId] = new HeatSource(i, j, 1.0f, 2.5f);
                        results[rIdx++] = checkSources(sourceId + 1, hSrcs, variation, iterations);
                    }
                }
                firstY = 0.0f;
            }
            if (size == 0) {
                return null;
            } else {
                for (int neighbor = 1; neighbor < results.length; neighbor *= 2) {
                    for (int idx = 0; idx + neighbor < results.length; idx += neighbor * 2) {
                        if (results[idx] == null) {
                            results[idx] = results[idx + neighbor];
                        } else if (results[idx + neighbor] != null) {
                            merge++;
                            results[idx] = Report.getBest(results[idx], results[idx + neighbor]);
                        }
                    }
                }
                return results[0];
            }
        }
    }

    private static int getMissing(float variation, float firstX, float firstY) {
        int steps = ((int) ((float) 1 / variation)) + 1;
        int missingX = (int) (((float) 1 - firstX) / variation);
        int missingY = (int) (((float) 1 - firstY) / variation);
        return (missingX * steps) + missingY + 1;
    }

    public static void main(String args[]) {
        int maxSources = Integer.parseInt(args[0]);
        float variation = Float.parseFloat(args[1]);
        int iterations = Integer.parseInt(args[2]);
        String report = Computation.start(maxSources, variation, iterations);
        System.out.println(report);
    }
}
