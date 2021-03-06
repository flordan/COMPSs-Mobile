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
import es.bsc.mobile.annotations.Constraints;
import es.bsc.mobile.annotations.CoreElement;
import es.bsc.mobile.annotations.JavaMethod;
import es.bsc.mobile.annotations.OpenCL;
import es.bsc.mobile.annotations.Parameter;
import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;

public interface CEI {

    @CoreElement(
        methods = @JavaMethod(  declaringClass = "es.bsc.mobile.apps.opencl.VectorOperations",
                                constraints = @Constraints(processorCoreCount = 4)),
        openclKernels = @OpenCL(kernel = "raw/kernels.cl", resultSize = {"par0.x"}, workloadSize = {"par0.x"})
    )
    float[] vecMultiplication(
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] a,
            @Parameter(type = Type.OBJECT, direction = Direction.IN) float[] b
    );

    @CoreElement(
        methods = @JavaMethod(declaringClass = "es.bsc.mobile.apps.opencl.VectorOperations"),
        openclKernels = @OpenCL(kernel = "raw/kernels.cl", workloadSize = {"par1.x"})
    )
    void scalarMultiplication(
            @Parameter(type = Type.FLOAT, direction = Direction.IN) float scalar,
            @Parameter(type = Type.OBJECT, direction = Direction.INOUT) float[] a
    );

}
