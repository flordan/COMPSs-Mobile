package es.bsc.mobile.apps.ece;

public class Operations {

    public static byte[] initialized(int[] xdims, float[] x, int[] conv1dims, float[] conv1, int[] conv2dims, float[] conv2, int[] fc1dims, float[] fc1, int[] fc2dims, float[] fc2, int[] adims, int[] bdims, int[] cdims, int[] ddims, int[] ddims2, int[] edims, int[] fdims) {
        System.out.println("Executed Initialized");
        return new byte[0];
    }

    /*
    public static float[][][] conv_forward_valid(float[][][] in, int[] inDims, int[][][][] conv, int[] convDims) {
        int[] outDims = new int[]{inDims[0] - convDims[0] + 1, inDims[1] - convDims[1] + 1, convDims[3]};
        float[][][] out = new float[inDims[0] - convDims[0] + 1][inDims[1] - convDims[1] + 1][convDims[3]];

        for (int pIdx = 0; pIdx < convDims[0]; pIdx++) {
            for (int qIdx = 0; qIdx < convDims[1]; qIdx++) {
                int[][] localConv = conv[pIdx][qIdx];

                for (int cIdx = 0; cIdx < convDims[2]; cIdx++) {
                    float[][] inputChannel = in[][][cIdx
                    ];
                    float[][][] outtmp = new float[outDims[0]][outDims[1]][outDims[2]];

                    for (int hIdx = 0; hIdx < outDims[0]; hIdx++) {
                        for (int wIdx = 0; wIdx < outDims[1]; wIdx++) {
                            float inValue = in[hIdx + pIdx][wIdx + qIdx][cIdx];
                            for (int mIdx = 0; mIdx < outDims[2]; mIdx++) {
                                out[hIdx][wIdx][mIdx] += inValue * localConv[cIdx][mIdx];
                            }
                        }
                    }
                }
            }
        }
        return out;
    }*/
    public static float[] conv_forward_3D_1channel(float[] x, int[] xDims, float[] w, int[] wDims, int[] outDims) {
        float[] out = new float[outDims[0] * outDims[1] * outDims[2] * outDims[3]];

        final int inImageSize = xDims[1] * xDims[2] * xDims[3];
        final int outImageSize = outDims[1] * outDims[2] * outDims[3];

        for (int imageId = 0; imageId < outDims[0]; imageId++) {
            int inImageOffset = imageId * inImageSize;
            int outImageOffset = imageId * outImageSize;

            for (int pIdx = 0; pIdx < wDims[0]; pIdx++) {
                for (int qIdx = 0; qIdx < wDims[1]; qIdx++) {
                    int localConvPtr = ((pIdx * wDims[1]) + qIdx) * wDims[2] * wDims[3];
                    for (int cIdx = 0; cIdx < wDims[2]; cIdx++) {
                        int inputChannelPtr = inImageOffset + cIdx + pIdx * xDims[2] * xDims[3] + qIdx * xDims[3];
                        int outOffset = 0;
                        for (int hIdx = 0; hIdx < outDims[1]; hIdx++) {
                            for (int wIdx = 0; wIdx < outDims[2]; wIdx++) {
                                float inValue = x[inputChannelPtr + hIdx * xDims[2] * xDims[3] + wIdx * xDims[3]];
                                for (int mIdx = 0; mIdx < outDims[3]; mIdx++) {
                                    float convValue = w[localConvPtr + cIdx * wDims[3] + mIdx];
                                    out[outImageOffset + outOffset] += inValue * convValue;
                                    outOffset++;
                                }
                            }
                        }
                    }
                }
            }
        }

        for (int i = 0; i < outDims[0] *outDims[1] * outDims[2] * outDims[3]; i++) {
            float val = out[i];
            out[i] = val > 0 ? val : 0;
        }

        /*int outOffset = 0;
        for (int imageId = 0; imageId < outDims[0]; imageId++) {
            int inImageOffset = imageId * inImageSize;
            int initialRowPtr = inImageOffset;
            for (int hIdx = 0; hIdx < outDims[1]; hIdx++) {
                int inInitialPtr = initialRowPtr;
                for (int wIdx = 0; wIdx < outDims[2]; wIdx++) {
                    for (int mIdx = 0; mIdx < outDims[3]; mIdx++) {
                        int maskPtr = mIdx;
                        float val = 0;

                        for (int pIdx = 0; pIdx < wDims[0]; pIdx++) {
                            for (int qIdx = 0; qIdx < wDims[1]; qIdx++) {
                                float inValue = x[inInitialPtr + pIdx * xDims[2] * xDims[3] + qIdx * xDims[3]];
                                float convValue = w[maskPtr];
                                val += inValue * convValue;
                                maskPtr += wDims[3];
                            }
                        }
                        out[outOffset] = val > 0 ? val : 0;
                        outOffset++;
                    }
                    inInitialPtr += xDims[3];
                }
                initialRowPtr += xDims[3] * xDims[2];
            }
        }*/
        return out;
    }

    public static float[] conv_forward_valid(float[] x, int[] xDims, float[] w, int[] wDims, int[] outDims) {
        float[] out = new float[outDims[0] * outDims[1] * outDims[2] * outDims[3]];

        final int inImageSize = xDims[1] * xDims[2] * xDims[3];
        final int outImageSize = outDims[1] * outDims[2] * outDims[3];

        for (int imageId = 0; imageId < outDims[0]; imageId++) {
            int inImageOffset = imageId * inImageSize;
            int outImageOffset = imageId * outImageSize;

            for (int pIdx = 0; pIdx < wDims[0]; pIdx++) {
                for (int qIdx = 0; qIdx < wDims[1]; qIdx++) {
                    int localConvPtr = ((pIdx * wDims[1]) + qIdx) * wDims[2] * wDims[3];
                    for (int cIdx = 0; cIdx < wDims[2]; cIdx++) {
                        int inputChannelPtr = inImageOffset + cIdx + pIdx * xDims[2] * xDims[3] + qIdx * xDims[3];

                        int outOffset = 0;
                        for (int hIdx = 0; hIdx < outDims[1]; hIdx++) {
                            for (int wIdx = 0; wIdx < outDims[2]; wIdx++) {
                                float inValue = x[inputChannelPtr + hIdx * xDims[2] * xDims[3] + wIdx * xDims[3]];
                                for (int mIdx = 0; mIdx < outDims[3]; mIdx++) {
                                    float convValue = w[localConvPtr + cIdx * wDims[3] + mIdx];
                                    out[outImageOffset + outOffset] += inValue * convValue;
                                    outOffset++;
                                }
                            }
                        }
                    }
                }
            }
        }
        return out;
    }

    public static float[] average_pool(float[] x, int[] xDims, int poolSize, int[] yDims) {
        System.out.println("Executing average_pool valid on CPU");
        float[] y = new float[yDims[0] * yDims[1] * yDims[2] * yDims[3]];

        final int x1d = xDims[3];
        final int x1dp = xDims[3] * poolSize;
        final int y1d = yDims[3];
        final int x2d = xDims[2] * x1d;
        final int x2dp = xDims[2] * x1dp;
        final int y2d = yDims[2] * y1d;
        final int x3d = xDims[1] * x2d;
        final int y3d = yDims[1] * y2d;
        int xIOffset = 0;
        int yIOffset = 0;
        for (int iIdx = 0; iIdx < yDims[0]; iIdx++) {
            int xHOffset = 0;
            int yHOffset = 0;
            for (int hIdx = 0; hIdx < yDims[1]; hIdx++) {
                int xWOffset = 0;
                int yWOffset = 0;
                for (int wIdx = 0; wIdx < yDims[2]; wIdx++) {

                    int threadxOffset = xIOffset + xHOffset + xWOffset;
                    int threadyOffset = yIOffset + yHOffset + yWOffset;
                    for (int mIdx = 0; mIdx < yDims[3]; mIdx++) {
                        int yoffset = threadyOffset + mIdx;
                        for (int pIdx = 0; pIdx < poolSize; pIdx++) {
                            for (int qIdx = 0; qIdx < poolSize; qIdx++) {
                                int xoffset = threadxOffset + pIdx * x2d + qIdx * x1d + mIdx;
                                y[yoffset] += (x[xoffset] > 0 ? x[xoffset] : 0) / (1.0f * poolSize * poolSize);
                            }
                        }
                    }

                    xWOffset += x1dp;
                    yWOffset += y1d;
                }
                xHOffset += x2dp;
                yHOffset += y2d;
            }
            xIOffset += x3d;
            yIOffset += y3d;
        }
        return y;
    }

    public static float[] relu2D(float[] x, int[] xDims) {
        System.out.println("Executing relu2D valid on CPU");

        int limit = xDims[0] * xDims[1];
        float[] out = new float[limit];
        for (int i = 0; i < limit; i++) {
            out[i] = (x[i] < 0) ? 0 : x[i];
        }
        return out;
    }

    public static float[] fully_forward(float[] x, int[] xDims, float[] w, int[] wDims, int[] yDims) {
        System.out.println("Executing fully_forward valid on CPU");

        float[] y = new float[yDims[0] * yDims[1]];
        for (int iIdx = 0; iIdx < xDims[0]; iIdx++) {
            for (int jIdx = 0; jIdx < wDims[1]; jIdx++) {
                float sum = 0;
                for (int kIdx = 0; kIdx < xDims[1]; kIdx++) {
                    sum += x[iIdx * xDims[1] + kIdx] * w[kIdx * wDims[1] + jIdx];
                }
                y[iIdx * wDims[1] + jIdx] = sum;
            }
        }
        return y;
    }

    public static int[] argmax(float[] f, int[] dims) {
        System.out.println("Executing argmax valid on CPU");

        int[] out = new int[dims[0]];
        Utils.argmax(f, dims, out);
        return out;

    }

}
