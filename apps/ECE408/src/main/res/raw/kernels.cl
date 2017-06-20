#define _OPENCL_COMPILER_

#pragma OPENCL EXTENSION cl_khr_global_int32_base_atomics : enable
#pragma OPENCL EXTENSION cl_khr_global_int32_extended_atomics : enable

#define MAX_TILE_WIDTH 32

__kernel void initialized(  __constant int* xdims, __global float *x,
                            __constant int *conv1dims, __constant float * conv1,
                            __constant int *conv2dims, __constant float * conv2, 
                            __constant int *fc1dims,  __constant float * fc1,
                            __constant int *fc2dims,  __constant float * fc2,
                            __constant int *adims,  __constant int *bdims, __constant int *cdims, __constant int *ddims, __constant int *ddims2, __constant int *edims, __constant int *fdims, __global unsigned char *out) {
    out[0] = 0;
}



__kernel void conv_forward_3D_1channel(__global float *x, __constant int *xDims, __constant float *w, __constant int *wDims, __constant int* outDims, __global float *out) {

    const int id = get_global_id(0);
    const int size = get_local_size(0);
    const int localId = get_local_id(0);
    
    const int imgId = id / size;
    const int imgRows = xDims[1];
    const int imgCols = xDims[2];
    const int inImgSize = imgRows * imgCols;
    const int inImgPtr = imgId * inImgSize;

    const int maskRows = wDims[0];
    const int maskCols = wDims[1];
    const int maskFeatures = wDims[3];
    const int maskSize= maskRows * maskCols * maskFeatures;

    const int outRows = outDims[1];
    const int outCols = outDims[2];
    const int outFeatures = outDims[3];
    const int outImageSize = outRows * outCols * outFeatures;
    const int outImgPtr = imgId * outImageSize;

    __local float img[32*32];
    __local float mask[32*32];

    for (int i = localId; i < maskSize; i += size){
        mask[i] = w[i];
    }

    for (int i = localId; i < inImgSize; i += size){
        img[i] = x[inImgPtr + i];
    }
    barrier(CLK_LOCAL_MEM_FENCE);

    for (int i=localId; i < outImageSize ; i += size){
        int outPixel = i / outFeatures;
        int outRow = outPixel / outCols;
        int outCol = outPixel % outCols;
        int outFeature = i % outFeatures;

        int inPtr = outRow * imgCols + outCol;
        float result = 0;
        int maskPos = outFeature;
        for (int maskRow=0; maskRow<maskRows; maskRow++){
            for (int maskCol=0; maskCol<maskRows; maskCol++){
                result += img[inPtr + maskCol] * mask[maskPos];
                maskPos += maskFeatures; 
            }
            inPtr += imgCols;
        }
        out[outImgPtr + i] = result > 0 ? result : 0;
    }
}


__kernel void conv_forward_valid(__global float *x, __constant int *xDims, __constant float *w, __constant int *wDims, __constant int* outDims, __global float *out) {

      const int id = get_global_id(0);
    const int size = get_local_size(0);
    const int localId = get_local_id(0);
    
    const int imgId = id / size;
    const int imgRows = xDims[1];
    const int imgCols = xDims[2];
    const int imgChannels = xDims[3];
    const int inImgPixels = imgRows * imgCols;
    const int inImgSize = imgRows * imgCols * imgChannels;
    const int inImgPtr = imgId * inImgSize;

    const int maskRows = wDims[0];
    const int maskCols = wDims[1];
    const int maskChannels = wDims[2];
    const int maskFeatures = wDims[3];
    const int maskPixelInfo = maskChannels * maskFeatures;
    const int channelMaskSize =  maskRows * maskCols *  maskFeatures;
    const int maskSize= channelMaskSize * maskChannels;

    const int outRows = outDims[1];
    const int outCols = outDims[2];
    const int outFeatures = outDims[3];
    const int outImageSize = outRows * outCols * outFeatures;
    const int outImgPtr = imgId * outImageSize;

    __local float img[12*12];
    __local float mask[25*64];

    for (int i=localId; i < outImageSize ; i += size){
        out[outImgPtr+i]=0;
    }

    for (int channelId = 0; channelId < maskChannels; channelId++){

        for (int i = localId; i < inImgPixels; i += size){
            img[i] = x[inImgPtr + i * imgChannels + channelId ];
        }

        for (int i = localId; i < channelMaskSize; i += size){
            int featureId= i % maskFeatures;
            int maskPixId = i/maskFeatures;
            mask[i] = w[maskPixId * maskPixelInfo + channelId * maskFeatures + featureId];
        }      
        barrier(CLK_LOCAL_MEM_FENCE);

        for (int i=localId; i < outImageSize ; i += size){
            int outPixel = i / outFeatures;
            int outRow = outPixel / outCols;
            int outCol = outPixel % outCols;
            int outFeature = i % outFeatures;

            int inPtr = outRow * imgCols + outCol;
            float result = 0;
            int maskPos = outFeature;
            for (int maskRow=0; maskRow<maskRows; maskRow++){
                for (int maskCol=0; maskCol<maskRows; maskCol++){
                    result += img[inPtr + maskCol] * mask[maskPos];
                    maskPos += maskFeatures; 
                }
                inPtr += imgCols;
            }
            out[outImgPtr + i] += result;
        }
        barrier(CLK_LOCAL_MEM_FENCE);
    }

    for (int i=localId; i < outImageSize ; i += size){
        float result = out[outImgPtr + i];
        out[outImgPtr + i] = result > 0 ? result : 0;
    }
}


__kernel void average_pool(__global float *x, __global int *xDims, int poolSize, __global int* outDims, __global float *out) {
    const int localId = get_local_id(0);
    const int size = get_local_size(0);

    const int x0 = xDims[0];
    const int inRows = xDims[1];
    const int inCols = xDims[2];
    const int inDepth = xDims[3];
    const int inRowSize = inCols * inDepth;
    const int inImageSize = inRows * inRowSize;
    

    const int outImages = xDims[0];
    const int outRows = outDims[1];
    const int outCols = outDims[2];
    const int outDepth = outDims[3];
    const int outRowSize = outCols * outDepth;
    const int outImageSize = outRows * outRowSize;
    const int outSize = outImages * outImageSize; 

    for (int i=localId; i < outSize; i+= size){
        int cellIdx = i % outImageSize;
        int colDepthId = cellIdx % outRowSize;

        int imageId = i / outImageSize;
        int row = cellIdx / outRowSize;
        int col = cellIdx / outDepth % outCols;
        int depth = cellIdx % outDepth;


        int inImgPtr = imageId * inImageSize + row * poolSize * inRowSize + col * poolSize * inDepth+ depth;
        float val = 0;
        int rowOffset=0;
        for (int poolRow = 0; poolRow < poolSize; poolRow++) {
            int colOffset=0;
            for (int poolCol = 0; poolCol < poolSize; poolCol++) {
                float pos =x[inImgPtr+ rowOffset + colOffset];
                val += pos > 0 ? pos: 0 ;
                colOffset+= inDepth;
            }
            rowOffset+=inRowSize;
        }
        out[i] = val /(1.0f *poolSize*poolSize);
    }
}


__kernel void relu2D(__global float *X, __global int *xdims, __global float *out) {
    int i = get_global_id(0);
    int total =  xdims[0]*xdims[1];
    if(i < total) {
        out[i] = (X[i] < 0) ? 0 : X[i];
    }
}

__kernel void fully_forward(__global float *X, __global int *xDims, __global float *W, __global int *wDims, __global int* outDims, __global float *out) {

    const int numXRows=xDims[0];
    const int numXColumns=xDims[1];
    const int numWRows=wDims[0];
    const int numWColumns=wDims[1];
    const int numOutRows=outDims[0];
    const int numOutColumns=outDims[1];

    //initialize shared memory
    __local float Xds[MAX_TILE_WIDTH][MAX_TILE_WIDTH];
    __local float Wds[MAX_TILE_WIDTH][MAX_TILE_WIDTH];

    //initialize block and thread index
    int tx =  get_local_id(0);;
    int ty =  get_local_id(1);
    int tile_width = get_local_size(0);

    //initialize row and col
    int Row =  get_global_id(1);
    int Col =  get_global_id(0);

    float sum = 0.0f;
    int offset = 0;
    if (numXColumns % tile_width) {
      offset = 1;
    }
    for(int ph = 0; ph < (numXColumns / tile_width) + offset; ++ph) {

        Xds[ty][tx] = X[Row * numXColumns + ph * tile_width+ tx];
        Wds[ty][tx] = W[(ph * tile_width+ ty) * numWColumns + Col];
        barrier(CLK_LOCAL_MEM_FENCE);

        for(int k = 0; k < tile_width; ++k) {
            if (
              (Row < numXRows) &&
              ((ph * tile_width + k) < numXColumns) &&
              ((ph * tile_width + k) < numWRows) &&
              (Col < numWColumns)
            ) {
                sum += Xds[ty][k] * Wds[k][tx];
            }
        }
        barrier(CLK_LOCAL_MEM_FENCE);

        if((Row < numOutRows) && (Col < numOutColumns)) {
            out[Row * numOutColumns + Col] = sum;
        }
    }
}

__kernel void argmax(__global float *X, __global int *xdims, __global int *out) {
    int i = get_global_id(0);
    int xdims0 = xdims[0];
    int xdims1 = xdims[1];

    if(i < xdims0) { // boundary check
        int max_idx = 0;
        float max = X[i * xdims1];
        for (int j = 0; j < xdims1; j++) {
            float elem = X[(i * xdims1) + j];
            if (elem > max) { // update max
                max_idx = j;
                max = elem;
            }
        }
        out[i] = max_idx;
    }
}