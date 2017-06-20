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
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ced.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"256"}, resultSize = {"1"})
    )
    byte[] initialized(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) byte[] data,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[][] gaus,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[][] sobx,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[][] soby
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ced.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par2-2", "par3-2"}, offset = {"1", "1"}, resultSize = {"par2 * par3"})
    )
    byte[] gaussian(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) byte[] data,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[][] gaus,
            @Parameter(type = Type.INT, direction = Direction.IN) int rows,
            @Parameter(type = Type.INT, direction = Direction.IN) int cols
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ced.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par1-2", "par2-2"}, offset = {"1", "1"}, resultSize = {"2*par1*par2"})
    )
    byte[] sobel(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) byte[] data,
            @Parameter(type = Type.INT, direction = Direction.IN) int rows,
            @Parameter(type = Type.INT, direction = Direction.IN) int cols,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[][] sobx,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) int[][] soby
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ced.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par1-2", "par2-2"}, offset = {"1", "1"}, resultSize = {"par1*par2"})
    )
    byte[] nonMaxSupp(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) byte[] data,
            @Parameter(type = Type.INT, direction = Direction.IN) int rows,
            @Parameter(type = Type.INT, direction = Direction.IN) int cols
    );

    @CoreElement(
            methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.ced.Operations"),
            openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par1-2", "par2-2"}, offset = {"1", "1"}, resultSize = {"par1*par2"})
    )
    byte[] hysteresis(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) byte[] data,
            @Parameter(type = Type.INT, direction = Direction.IN) int rows,
            @Parameter(type = Type.INT, direction = Direction.IN) int cols
    );
}
