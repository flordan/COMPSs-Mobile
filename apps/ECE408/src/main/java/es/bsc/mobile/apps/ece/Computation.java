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
package es.bsc.mobile.apps.ece;

import es.bsc.mobile.annotations.Orchestration;
import java.lang.reflect.Array;

@Orchestration
public class Computation {

    private final static int NUM_ROWS = 28;
    private final static int NUM_COLS = 28;
    private final static int NUM_CHANNELS = 1;
    private final static int NUM_SIZE = NUM_ROWS * NUM_COLS * NUM_CHANNELS;
    private final static int NUM_DIGITS = 10;

    public static void start(int batchSize) throws Exception {

        // Model dimensions
        int[] CONV1_DIMS = new int[]{5, 5, 1, 32};
        int[] CONV2_DIMS = new int[]{5, 5, 32, 64};
        int[] FC1_DIMS = new int[]{1024, 128};
        int[] FC2_DIMS = new int[]{128, 10};

        int CONV1_SIZE = CONV1_DIMS[0] * CONV1_DIMS[1] * CONV1_DIMS[2] * CONV1_DIMS[3];
        int CONV2_SIZE = CONV2_DIMS[0] * CONV2_DIMS[1] * CONV2_DIMS[2] * CONV2_DIMS[3];
        int FC1_SIZE = FC1_DIMS[0] * FC1_DIMS[1];
        int FC2_SIZE = FC2_DIMS[0] * FC2_DIMS[1];

        // Data and reference data dimensions
        int xdims[] = new int[]{batchSize, NUM_ROWS, NUM_COLS, NUM_CHANNELS};
        int rdims[] = new int[]{batchSize, NUM_DIGITS};

        float[] x = new float[batchSize * NUM_SIZE];
        float[] y = new float[batchSize * NUM_SIZE];
        Utils.loadData(batchSize, x, y);

        float[] conv1 = new float[CONV1_SIZE];
        float[] conv2 = new float[CONV2_SIZE];
        float[] fc1 = new float[FC1_SIZE];
        float[] fc2 = new float[FC2_SIZE];
        Utils.loadModel(conv1, conv2, fc1, fc2);

        // Perform foward opertion
        //Steps dimensions
        int pool_size = 2;

        int adims[] = new int[]{batchSize, (xdims[1] - CONV1_DIMS[0] + 1), (xdims[2] - CONV1_DIMS[1] + 1), CONV1_DIMS[3]};
        int bdims[] = new int[]{batchSize, adims[1] / pool_size, adims[2] / pool_size, adims[3]};
        int cdims[] = new int[]{batchSize, (bdims[1] - CONV2_DIMS[0] + 1), (bdims[2] - CONV2_DIMS[1] + 1), CONV2_DIMS[3]};
        int ddims[] = new int[]{batchSize, cdims[1] / pool_size, cdims[2] / pool_size, cdims[3]};
        int ddims2[] = new int[]{batchSize, ddims[1] * ddims[2] * ddims[3]};
        int edims[] = new int[]{batchSize, FC1_DIMS[1]};
        int fdims[] = new int[]{batchSize, FC2_DIMS[1]};

        byte[] initiliazed = Operations.initialized(xdims, x, CONV1_DIMS, conv1, CONV2_DIMS, conv2, FC1_DIMS, fc1, FC2_DIMS, fc2, adims, bdims, cdims, ddims, ddims2, edims, fdims);
        int init = Array.getLength(initiliazed);

        long start = System.currentTimeMillis();

        System.out.println(System.currentTimeMillis() + " Runtime is ready");
        // conv layer
        //float[] a = Operations.conv_forward_valid(x, xdims, conv1, CONV1_DIMS, adims);
        float[] a = Operations.conv_forward_3D_1channel(x, xdims, conv1, CONV1_DIMS, adims);
        
        // average pooling
        float[] b = Operations.average_pool(a, adims, pool_size, bdims);

        // conv layer
        float[] c = Operations.conv_forward_valid(b, bdims, conv2, CONV2_DIMS, cdims);

        // average pooling
        float[] d = Operations.average_pool(c, cdims, pool_size, ddims);

        // reshape
        // matrix multiplication
        float[] e = Operations.fully_forward(d, ddims2, fc1, FC1_DIMS, edims);

        // relu
        e = Operations.relu2D(e, edims);

        // matrix multiplication
        final float[] f = Operations.fully_forward(e, edims, fc2, FC2_DIMS, fdims);

        int[] out = Operations.argmax(f, fdims);
        int i = Array.getLength(out);
        final long end = System.currentTimeMillis();

        long elapsed = end - start;

        int numCorrect = Utils.validate(out, y, rdims);
        System.out.println("Done with " + batchSize + " queries in elapsed = " + elapsed + " milliseconds. Correctness: " + ((float) numCorrect / (float) batchSize));
    }

    public static void main(String args[]) throws Exception {
        Integer batchSize = 1000;

        /*int argc = args.length;
        if (argc != 2 && argc != 3) {
            System.err.println("This program performs the forward opertion step for Convolutional Neural Network(CNN).  Sample usage: \n"
                    + "java es.bsc.mobile.apps.ece.Computation [../data/test10.hdf5] [../data/model.hdf5] [10]");
            return;
        }
        
        String testdata = args[0];
        String model = args[1];
        Integer batchSize = 0;
        if (argc == 2) {
            batchSize = DEFAULT_BATCH_SIZES.get(testdata);
            if (batchSize == null) {
                System.err.println("ERROR::Unrecognized file " + testdata + " batch_size must be specified.");
                return;
            }
        } else if (argc == 3) {
            batchSize = Integer.parseInt(args[2]);
        }*/
        Computation.start(batchSize);
    }

}
