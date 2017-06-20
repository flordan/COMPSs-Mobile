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
package es.bsc.mobile.apps.ced;

import es.bsc.mobile.annotations.Orchestration;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Array;

@Orchestration
public class Computation {

    private static final float[][] GAUSS = {{0.0625f, 0.125f, 0.0625f}, {0.1250f, 0.250f, 0.1250f}, {0.0625f, 0.125f, 0.0625f}};
    private static final int[][] SOBX = {{-1, 0, 1}, {-2, 0, 2}, {-1, 0, 1}};
    private static final int[][] SOBY = {{-1, -2, -1}, {0, 0, 0}, {1, 2, 1}};

    public static void main(String args[]) {
        System.out.println("Inici: " + Runtime.getRuntime().freeMemory());
        Params p = new Params();
        byte[][] allGrayFrames = new byte[p.getnWarmup() + p.getReps()][];
        int[] rowscArray = new int[1];
        int[] colscArray = new int[1];
        readInput(allGrayFrames, rowscArray, colscArray, p);
        int rowsc = rowscArray[0];
        int colsc = colscArray[0];
        System.out.println("Final Read input: " + Runtime.getRuntime().freeMemory());

        byte[][] outFrames = new byte[p.getnWarmup() + p.getReps()][];

        System.out.println("Reserva temporals: " + Runtime.getRuntime().freeMemory());

        //Heat up runtime
        for (int rep = 0; rep < p.getnWarmup(); rep++) {
            long time = System.currentTimeMillis();
            byte[] grayFrameHeat = allGrayFrames[rep];
            byte[] intermHeat = Operations.gaussian(grayFrameHeat, GAUSS, rowsc, colsc);
            byte[] thetaHeat = Operations.sobel(intermHeat, rowsc, colsc, SOBX, SOBY);
            intermHeat = Operations.nonMaxSupp(thetaHeat, rowsc, colsc);
            outFrames[rep] = Operations.hysteresis(intermHeat, rowsc, colsc);
            int i = Array.getLength(outFrames[rep]);
            System.out.println(System.currentTimeMillis() - time);
        }
        //Moving input data into the runtime process
        for (int rep = p.getnWarmup(); rep < p.getnWarmup() + p.getReps(); rep++) {
            byte[] sync = Operations.initialized(allGrayFrames[rep], GAUSS, SOBX, SOBY);
            int i = Array.getLength(sync);
        }
        System.out.println("Runtime is ready ;)");

        long startTime = System.currentTimeMillis();
        for (int rep = p.getnWarmup(); rep < p.getnWarmup() + p.getReps(); rep++) {
            byte[] grayFrameHeat = allGrayFrames[rep];
            byte[] intermHeat = Operations.gaussian(grayFrameHeat, GAUSS, rowsc, colsc);
            byte[] thetaHeat = Operations.sobel(intermHeat, rowsc, colsc, SOBX, SOBY);
            intermHeat = Operations.nonMaxSupp(thetaHeat, rowsc, colsc);
            outFrames[rep] = Operations.hysteresis(intermHeat, rowsc, colsc);
        }

        System.out.println("All tasks generated:" + (System.currentTimeMillis() - startTime));

        for (int rep = p.getnWarmup(); rep < p.getnWarmup() + p.getReps(); rep++) {
            int i = Array.getLength(outFrames[rep]);
        }
        System.out.println("All tasks finished:" + (System.currentTimeMillis() - startTime));

        System.out.println("Starting Verification");
        int numFrames = p.getnWarmup() + p.getReps();
        // Compare to output file
        InputStream reference = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/result.txt");
        DataInputStream dis = new DataInputStream(reference);
        int count_error = 0;
        try {
            for (int rep = 0; rep < numFrames; rep++) {
                count_error += Operations.validate(outFrames[rep], rowsc, colsc, dis);
            }
            reference.close();
        } catch (IOException ex) {

        }
        if ((float) count_error / (float) (rowsc * colsc * numFrames) < 1e-1) {
            System.out.println("Pass\n");
        } else {
            System.out.println("Fail\n");
        }

        //System.out.println("All tasks executed:" + elapsedTime);
        /*if (compareOutput(outFrames, p.getnWarmup() + p.getReps(), rowsc, colsc)) {
            System.out.println("Pass\n");
        } else {
            System.out.println("Fail\n");
        }*/
    }

    private static void readInput(byte[][] allGrayFrames, int[] rowsc, int[] colsc, Params p) {
        // Load input frames
        for (int taskId = 0; taskId < p.getnWarmup() + p.getReps(); taskId++) {
            try {
                InputStream file = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/frame" + taskId + ".txt");
                BufferedReader br = new BufferedReader(new InputStreamReader(file));
                String rowsText = br.readLine();
                rowsc[0] = Integer.parseInt(rowsText);
                String colsText = br.readLine();
                colsc[0] = Integer.parseInt(colsText);
                br.close();
                file.close();

                int inSize = rowsc[0] * colsc[0];
                byte[] buff = new byte[inSize];
                file = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/frame" + taskId + ".txt");
                for (int nl = 0; nl < 2;) {
                    byte b = (byte) file.read();
                    if (b == '\n') {
                        nl++;
                    }
                }
                file.read(buff);
                file.close();
                byte[] frame = new byte[rowsc[0] * colsc[0]];
                System.arraycopy(buff, 0, frame, 0, rowsc[0] * colsc[0]);
                allGrayFrames[taskId] = frame;
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    private static boolean compareOutput(byte[][][] allOutFrames, int numFrames, int rows, int cols) {
        System.out.println("Starting Verification");
        // Compare to output file
        InputStream reference = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/result.txt");
        DataInputStream dis = new DataInputStream(reference);
        int count_error = 0;
        try {

            for (int frame = 0; frame < numFrames; frame++) {
                for (int row = 0; row < rows; row++) {
                    for (int col = 0; col < cols; col++) {
                        byte pix = dis.readByte();
                        if (allOutFrames[frame][row][col] != pix) {
                            count_error++;
                        }
                    }
                }
            }

            reference.close();
        } catch (IOException ex) {

        }
        return ((float) count_error / (float) (rows * cols * numFrames) < 1e-1);
    }

}
