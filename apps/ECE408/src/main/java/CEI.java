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
import es.bsc.mobile.annotations.CoreElement;
import es.bsc.mobile.annotations.JavaMethod;
import es.bsc.mobile.annotations.OpenCL;
import es.bsc.mobile.annotations.Parameter;
import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;

public interface CEI {

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"256"}, resultSize = {"1"})
    )
    byte[] initialized(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xdims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] conv1Dims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] conv1,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] conv2Dims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] conv2,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] fc1Dims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] fc1,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] fc2Dims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] fc2,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] adims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] bdims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] cdims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] ddims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] ddims2,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] edims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] fdims
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par4[0]*256"}, localSize = {"256"}, resultSize = {"par4[0] * par4[1] * par4[2] * par4[3]"})
    )
    float[] conv_forward_3D_1channel(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] w,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] wDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] yDims
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par4[0]*256"}, localSize = {"256"}, resultSize = {"par4[0] * par4[1] * par4[2] * par4[3]"})
    )
    float[] conv_forward_valid(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] w,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] wDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] yDims
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par1[0] * par1[1]"}, resultSize = {"par1[0] * par1[1]"})
    )
    float[] relu2D(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xdims
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"256"}, localSize = {"256"}, resultSize = {"par3[0] * par3[1] * par3[2] * par3[3]"})
    )
    float[] average_pool(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xDims,
            @Parameter(type = Type.INT, direction = Direction.IN) int poolSize,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] yDims
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"((par4[1]+16-1)/16)*16 ", "((par4[0]+16-1)/16)*16 "}, resultSize = {"par4[0]*par4[1]"})
    )
    float[] fully_forward(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] x,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] xDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] w,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] wDims,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] yDims);

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ece.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"((par1[0]+256-1)/256)*256"}, resultSize = {"par1[0]"})
    )
    int[] argmax(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] f,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[] fdims);
}
