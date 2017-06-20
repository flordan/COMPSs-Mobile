/*
 * Copyright (c) 2016 University of Cordoba and University of Illinois
 * All rights reserved.
 *
 * Developed by:    IMPACT Research Group
 *                  University of Cordoba and University of Illinois
 *                  http://impact.crhc.illinois.edu/
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the 
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 *      > Redistributions of source code must retain the above copyright notice,
 *        this list of conditions and the following disclaimers.
 *      > Redistributions in binary form must reproduce the above copyright
 *        notice, this list of conditions and the following disclaimers in the
 *        documentation and/or other materials provided with the distribution.
 *      > Neither the names of IMPACT Research Group, University of Cordoba, 
 *        University of Illinois nor the names of its contributors may be used 
 *        to endorse or promote products derived from this Software without 
 *        specific prior written permission.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
 * CONTRIBUTORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS WITH
 * THE SOFTWARE.
 *
 */

#define _OPENCL_COMPILER_

#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics : enable


// BezierBlend (http://paulbourke.net/geometry/bezier/)
float BezierBlendGPU(int k, float mu, int n) {
    int nn, kn, nkn;
    float   blend = 1;
    nn        = n;
    kn        = k;
    nkn       = n - k;
    while(nn >= 1) {
        blend *= nn;
        nn--;
        if(kn > 1) {
            blend /= (float)kn;
            kn--;
        }
        if(nkn > 1) {
            blend /= (float)nkn;
            nkn--;
        }
    }
    if(k > 0)
        blend *= pow(mu, (float)k);
    if(n - k > 0)
        blend *= pow(1 - mu, (float)(n - k));
    return (blend);
}

// OpenCL kernel --------------------------------------------------------------
__kernel void processTile(__global float *in, int inRows, int inCols, int outRows, int outCols, int tileIdI, int tileIdJ, int tileSize, __global float *out) {
    __local float l_in[1000];
    int myIdy = get_local_id(0);
    int groupSizeCols = get_local_size(0);
    int myIdx = get_local_id(1);
    int groupSizeRows = get_local_size(1);

    for(int i = myIdx * groupSizeCols + myIdy; i < (inRows + 1) * (inCols + 1) * 3; i += groupSizeCols * groupSizeRows)
        l_in[i] = in[i];
    barrier(CLK_LOCAL_MEM_FENCE);

        int initialRow = tileIdI * tileSize;
        int initialCol = tileIdJ * tileSize;

        for (int tileRow = myIdy; tileRow < tileSize; tileRow+=groupSizeRows) {
            int row = initialRow + tileRow;
            float mui = (float) row / (float) (outRows - 1);
            for (int tileCol = myIdx; tileCol < tileSize; tileCol+=groupSizeCols) {
                int col = initialCol + tileCol;
                float muj = (float) col / (float) (outCols - 1);

                if (row < outRows && col < outCols) {
                    float x;
                    float y;
                    float z;
                    #pragma unroll
                    for (int ki = 0; ki <= inRows; ki++) {
                        float bi = BezierBlendGPU(ki, mui, inRows);
                        #pragma unroll
                        for (int kj = 0; kj <= inCols; kj++) {
                            float bj = BezierBlendGPU(kj, muj, inCols);
                            x += (l_in[(ki * (inCols + 1) + kj) * 3] * bi * bj);
                            y += (l_in[(ki * (inCols + 1) + kj) * 3 + 1] * bi * bj);
                            z += (l_in[(ki * (inCols + 1) + kj) * 3 + 2] * bi * bj);
                        }
                    }

                    out[(tileRow * tileSize + tileCol) * 3] = x;
                    out[(tileRow * tileSize + tileCol) * 3 + 1] = y;
                    out[(tileRow * tileSize + tileCol) * 3 + 2] = z;
                }
            }
        }



}
