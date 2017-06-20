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
package es.bsc.mobile.apps.opencl;

import es.bsc.mobile.annotations.Orchestration;

@Orchestration
public class Computation {

    public static void main(String args[]) {
        int n = 10;
        float[] dataA = new float[n];
        float[] dataB = new float[n];
        for (int i = 0; i < n; i++) {
            dataA[i] = i;
            dataB[i] = i;
        }

        float[] dstArray = VectorOperations.vecMultiplication(dataA, dataB);
        dstArray = VectorOperations.vecMultiplication(dstArray, dataB);
        VectorOperations.scalarMultiplication(3f, dstArray);


        //Check result
        boolean passed = true;
        final float epsilon = 1e-7f;
        for (int i = 0; i < n; i++) {
            float x = dstArray[i];
            float y = dataA[i] * dataB[i] * dataB[i] * 3;
            boolean epsilonEqual = Math.abs(x - y) <= epsilon * Math.abs(x);
            if (!epsilonEqual) {
                passed = false;
                break;
            }
        }

        //Print result
        System.out.println("Test " + (passed ? "PASSED" : "FAILED"));
        if (n <= 10) {
            StringBuilder sb = new StringBuilder("Result: [");
            int idx = 0;
            if (n > 0) {
                sb.append(dstArray[0]);
            }
            idx++;
            for (; idx < n; idx++) {
                sb.append(" ").append(dstArray[idx]);
            }
            sb.append("]");
            System.out.println(sb.toString());
        }
    }
}
