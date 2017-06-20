/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package es.bsc.mobile.apps.bs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 *
 * @author flordan
 */
public class Utils {

    public static void loadData(float[] in, Params p) throws FileNotFoundException, IOException, ClassNotFoundException {
        InputStream fis = ClassLoader.getSystemClassLoader().getResourceAsStream("raw/control.txt");
        BufferedReader r = new BufferedReader(new InputStreamReader(fis));
        // Store points from input file to array
        int k = 0;
        int ic = 0;
        float[][] read = new float[10000][3];
        String line;
        while ((line = r.readLine()) != null) {
            String[] entry = line.split(",");
            read[ic][0] = Float.parseFloat(entry[0]);
            read[ic][1] = Float.parseFloat(entry[1]);
            read[ic][2] = Float.parseFloat(entry[2]);
            ic++;
        }

        for (int i = 0; i <= p.getInRows(); i++) {
            for (int j = 0; j <= p.getInCols(); j++) {

                in[(i * (p.getInCols() + 1) + j) * 3] = read[k][0];
                in[(i * (p.getInCols() + 1) + j) * 3 + 1] = read[k][1];
                in[(i * (p.getInCols() + 1) + j) * 3 + 2] = read[k][2];
                //k++;
                k = (k + 1) % 16;
            }
        }
    }

    private Utils() {

    }

    public static void BezierCPU(float[] inp, float[] outp, int NI, int NJ, int RESOLUTIONI, int RESOLUTIONJ) {
        int i, j, ki, kj;
        float mui, muj, bi, bj;
        for (i = 0; i < RESOLUTIONI; i++) {
            mui = i / (float) (RESOLUTIONI - 1);
            for (j = 0; j < RESOLUTIONJ; j++) {
                muj = j / (float) (RESOLUTIONJ - 1);
                float[] out = {0, 0, 0};
                for (ki = 0; ki <= NI; ki++) {
                    bi = BezierBlend(ki, mui, NI);
                    for (kj = 0; kj <= NJ; kj++) {
                        bj = BezierBlend(kj, muj, NJ);
                        out[0] += (inp[(ki * (NJ + 1) + kj) * 3] * bi * bj);
                        out[1] += (inp[(ki * (NJ + 1) + kj) * 3 + 1] * bi * bj);
                        out[2] += (inp[(ki * (NJ + 1) + kj) * 3 + 2] * bi * bj);
                    }
                }
                outp[(i * RESOLUTIONJ + j) * 3] = out[0];
                outp[(i * RESOLUTIONJ + j) * 3 + 1] = out[1];
                outp[(i * RESOLUTIONJ + j) * 3 + 2] = out[2];
            }
        }
    }

    public static float BezierBlend(int k, float mu, int n) {
        float nn, kn, nkn;
        float blend = 1;
        nn = n;
        kn = k;
        nkn = n - k;
        while (nn >= 1) {
            blend *= nn;
            nn--;
            if (kn > 1) {
                blend /= kn;
                kn--;
            }
            if (nkn > 1) {
                blend /= nkn;
                nkn--;
            }
        }
        if (k > 0) {
            blend *= Math.pow(mu, k);
        }
        if (n - k > 0) {
            blend *= Math.pow(1 - mu, (n - k));
        }
        return (blend);
    }

    public static double[] validateTyle(float[] candidate, float[] gold, int RESOLUTIONI, int RESOLUTIONJ, int tileSize, int tileI, int tileJ) {
        double sum_ref2 = 0;
        double sum_delta2 = 0;

        for (int i = 0; i < tileSize && tileI * tileSize + i < RESOLUTIONI; i++) {
            int rowOffset = (tileI*tileSize +i )* RESOLUTIONJ ;
            for (int j = 0; j < tileSize && tileJ * tileSize + j < RESOLUTIONJ; j++) {
                int colOffset = tileJ * tileSize + j;
                int position = (rowOffset + colOffset) * 3;

                sum_ref2 += Math.abs(candidate[(i * tileSize + j) * 3]);
                sum_delta2 += Math.abs(candidate[(i * tileSize + j) * 3] - gold[position]);
                sum_ref2 += Math.abs(candidate[(i * tileSize + j) * 3 + 1]);
                sum_delta2 += Math.abs(candidate[(i * tileSize + j) * 3 + 1] - gold[position + 1]);
                sum_ref2 += Math.abs(candidate[(i * tileSize + j) * 3 + 2]);
                sum_delta2 += Math.abs(candidate[(i * tileSize + j) * 3 + 2] - gold[position + 2]);
            }
        }
        return new double[]{sum_delta2, sum_ref2};
    }
}
