/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.mobile.apps.ece;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

/**
 *
 * @author flordan
 */
public class Utils {

    private Utils() {

    }

    public static int validate(int[] out, float[] y, int[] rdims) {
        int batchSize = out.length;
        // Get reference
        int[] ref = new int[batchSize];
        Utils.argmax(y, rdims, ref);
        // Calculate correctness
        int numCorrect = 0;
        for (int i = 0; i < batchSize; i++) {
            if (out[i] == ref[i]) {
                numCorrect++;
            }
        }
        return numCorrect;
    }

    public static void loadData(int batchSize, final float[] x, final float[] y) throws FileNotFoundException, IOException, ClassNotFoundException {
        long[] input_dims;
        float[] tmpx;
        float[] tmpy;

        InputStream fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/dims");
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(fis);
            input_dims = (long[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }

        fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/x");
        ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmpx = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }

        fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/y");
        ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmpy = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }

        if (input_dims[0] < batchSize) {
            System.err.println("Batch Size is larger than input size!!!!");
            System.exit(1);
        } else {
            int numSize = (int) (input_dims[1] * input_dims[2] * input_dims[3]);
            System.arraycopy(tmpx, 0, x, 0, numSize * batchSize);
            System.arraycopy(tmpy, 0, y, 0, numSize * batchSize);
        }

        System.out.println("input dimensions = " + input_dims[0] + " x " + input_dims[1] + " x " + input_dims[2] + " x " + input_dims[3]);
        System.out.println("Final dimensions = " + batchSize + " x " + input_dims[1] + " x " + input_dims[2] + " x " + input_dims[3]);
    }

    public static void loadModel(final float[] conv1, final float[] conv2, final float[] fc1, final float[] fc2) throws IOException, ClassNotFoundException {
        float[] tmp;
        InputStream fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/conv1");
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmp = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }
        System.arraycopy(tmp, 0, conv1, 0, tmp.length);

        fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/conv2");
        ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmp = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }
        System.arraycopy(tmp, 0, conv2, 0, tmp.length);

        fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/fc1");
        ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmp = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }
        System.arraycopy(tmp, 0, fc1, 0, tmp.length);
        fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/fc2");
        ois = null;
        try {
            ois = new ObjectInputStream(fis);
            tmp = (float[]) ois.readObject();
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {

                //Error closing the output stream. Don't do anything
            }
            try {
                fis.close();
            } catch (IOException e) {
                //Error closing the output stream. Don't do anything
            }
        }
        System.arraycopy(tmp, 0, fc2, 0, tmp.length);
    }

    /*
    public static void loadData(String testdata, int batchSize, final float[] x, final float[] y) throws HDF5LibraryException, HDF5Exception, FileNotFoundException {
        // Open the data file
        final int file_id = H5.H5Fopen(testdata, H5F_ACC_RDWR, H5P_DEFAULT);

        // Open the dataset x and y
        final int xId = H5.H5Dopen(file_id, "/x");

        final int yId = H5.H5Dopen(file_id, "/y");
        // Get the dataset x dimensions

        final int xspace = H5.H5Dget_space(xId);
        final int xndims = H5.H5Sget_simple_extent_ndims(xspace);
        assert (xndims == 4);

        long[] input_dims = new long[xndims];
        H5.H5Sget_simple_extent_dims(xspace, input_dims, null);
        if (input_dims[0] == batchSize) {
            // Read the dataset x and y
            H5.H5Dread(xId, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, x);
            H5.H5Dread(yId, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, y);
        } else if (input_dims[0] > batchSize) {
            int numSize = (int) (input_dims[1] * input_dims[2] * input_dims[3]);
            float[] tmp = new float[(int) (input_dims[0] * numSize)];
            H5.H5Dread(xId, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, tmp);
            System.arraycopy(tmp, 0, x, 0, numSize * batchSize);

            tmp = new float[(int) (input_dims[0] * numSize)];
            H5.H5Dread(yId, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, tmp);
            System.arraycopy(tmp, 0, y, 0, numSize * batchSize);
        } else {
            System.err.println("Batch Size is larger than input size!!!!");
            System.exit(1);
        }
        System.out.println("input dimensions = " + input_dims[0] + " x " + input_dims[1] + " x " + input_dims[2] + " x " + input_dims[3]);
        System.out.println("Final dimensions = " + batchSize + " x " + input_dims[1] + " x " + input_dims[2] + " x " + input_dims[3]);

        // Close the dataset x and y
        H5.H5Dclose(xId);
        H5.H5Dclose(yId);
        
        // Close the file
        H5.H5Fclose(file_id);
    }

    public static void loadModel(String model, final float[] conv1, final float[] conv2, final float[] fc1, final float[] fc2) throws HDF5LibraryException, HDF5Exception {
        // Open the model file
        final int file_id = H5.H5Fopen(model, H5F_ACC_RDWR, H5P_DEFAULT);
        // Open the dataset
        final int conv1_id = H5.H5Dopen(file_id, "/conv1");
        final int conv2_id = H5.H5Dopen(file_id, "/conv2");
        final int fc1_id = H5.H5Dopen(file_id, "/fc1");
        final int fc2_id = H5.H5Dopen(file_id, "/fc2");

        // Read the dataset
        H5.H5Dread(conv1_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, conv1);
        H5.H5Dread(conv2_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, conv2);
        H5.H5Dread(fc1_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, fc1);
        H5.H5Dread(fc2_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, fc2);

        // Close the dataset x and y
        H5.H5Dclose(conv1_id);
        H5.H5Dclose(conv2_id);
        H5.H5Dclose(fc1_id);
        H5.H5Dclose(fc2_id);

        // Close the file
        H5.H5Fclose(file_id);
    }
     */
    // Choose the guess with largest score
    public static void argmax(final float[] x, final int[] xdims, final int[] y) {
        final int limitX = xdims[0];
        for (int i = 0; i < limitX; i++) {
            int maxIdx = 0;
            float max = x[i * xdims[1]];
            final int limitY = xdims[1];
            for (int j = 0; j < limitY; j++) {
                final float elem = x[(i * xdims[1]) + j];
                if (elem > max) {
                    maxIdx = j;
                    max = elem;
                }
            }
            y[i] = maxIdx;
        }
    }

}
