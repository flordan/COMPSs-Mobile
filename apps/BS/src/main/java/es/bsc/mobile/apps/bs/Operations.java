package es.bsc.mobile.apps.bs;

public class Operations {

    public static float[] processTile(float[] in, int inRows, int inCols, int outRows, int outCols, int tileIdI, int tileIdJ, int tileSize) {
        int initialRow = tileIdI * tileSize;
        int initialCol = tileIdJ * tileSize;
        float[] out = new float[tileSize * tileSize * 3];

        for (int tileRow = 0; tileRow < tileSize; tileRow++) {
            int row = initialRow + tileRow;
            float mui = (float) row / (float) (outRows - 1);
            for (int tileCol = 0; tileCol < tileSize; tileCol++) {
                int col = initialCol + tileCol;
                float muj = (float) col / (float) (outCols - 1);

                if (row < outRows && col < outCols) {
                    float[] outTmp = new float[]{0, 0, 0};
                    for (int ki = 0; ki <= inRows; ki++) {
                        float bi = Utils.BezierBlend(ki, mui, inRows);
                        for (int kj = 0; kj <= inCols; kj++) {
                            float bj = Utils.BezierBlend(kj, muj, inCols);
                            outTmp[0] += (in[(ki * (inCols + 1) + kj) * 3] * bi * bj);
                            outTmp[1] += (in[(ki * (inCols + 1) + kj) * 3 + 1] * bi * bj);
                            outTmp[2] += (in[(ki * (inCols + 1) + kj) * 3 + 2] * bi * bj);
                        }
                    }

                    out[(tileRow * tileSize + tileCol) * 3] = outTmp[0];
                    out[(tileRow * tileSize + tileCol) * 3 + 1] = outTmp[1];
                    out[(tileRow * tileSize + tileCol) * 3 + 2] = outTmp[2];
                }
            }
        }
        return out;
    }

    public static boolean verify(float[] in, float[][][] out, int inRows, int inCols, int outRows, int outCols, int tileSize) {
        float[] gold = new float[outRows * outCols * 3];
        Utils.BezierCPU(in, gold, inRows, inCols, outRows, outRows);
        double error = compareOutputs(out, gold, outRows, outRows, tileSize);
        return (error < 1e-6);
    }

    private static double compareOutputs(float[][][] outp, float[] outpCPU, int RESOLUTIONI, int RESOLUTIONJ, int tileSize) {
        double sum_delta2 = 0;
        double sum_ref2 = 0;

        for (int i = 0; i < RESOLUTIONI; i++) {
            for (int j = 0; j < RESOLUTIONJ; j++) {

                int tileI = i / tileSize;
                int tileJ = j / tileSize;
                int tileRow = i % tileSize;
                int tileCol = j % tileSize;

                sum_delta2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3] - outpCPU[(i * RESOLUTIONJ + j) * 3]);
                sum_ref2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3]);
                sum_delta2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3 + 1] - outpCPU[(i * RESOLUTIONJ + j) * 3 + 1]);
                sum_ref2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3 + 1]);
                sum_delta2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3 + 2] - outpCPU[(i * RESOLUTIONJ + j) * 3 + 2]);
                sum_ref2 += Math.abs(outp[tileI][tileJ][(tileRow * tileSize + tileCol) * 3 + 2]);

            }
        }
        return (sum_delta2 / sum_ref2);
    }
}
