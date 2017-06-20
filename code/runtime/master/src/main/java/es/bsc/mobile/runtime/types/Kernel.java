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
package es.bsc.mobile.runtime.types;

import java.io.IOException;
import java.io.InputStream;

public class Kernel extends Implementation {

    private final String program;
    private final String code;
    private final String[] resultSize;
    private final String[] workloadSize;
    private final String[] localSize;
    private final String[] offset;
    private final Class<?> resultType;

    public Kernel(int coreElementId, int implementationId, String program, String methodName, Class<?> resultType, String[] resultSize, String[] workloadSize, String[] localSize, String[] offset) {
        super(coreElementId, implementationId, methodName);
        this.program = program;
        InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream(program);
        String code = null;
        if (input != null) {
            try {
                int size = input.available();
                byte[] content = new byte[size];
                input.read(content);
                code = new String(content);
            } catch (IOException ioe) {
                code = null;
            }
        }
        this.code = code;
        Class<?> rType = resultType;
        int dims = 0;
        while (rType.isArray()) {
            rType = rType.getComponentType();
            dims++;
        }
        this.resultType = rType;
        if (resultSize.length == dims) {
            this.resultSize = resultSize;
        } else {
            throw new RuntimeException("Dimensions of result for the method and for annotations does not match. Annotation provided " + resultSize.length + " expressions and method generates a " + dims + "-dimension matrix");
        }
        this.workloadSize = workloadSize;
        this.localSize = localSize;
        this.offset = offset;
    }

    public String getProgram() {
        return program;
    }

    @Override
    public String completeSignature(String methodSignature) {
        return program+"->"+methodSignature;
    }

    public String getSourceCode() {
        return code;
    }

    @Override
    public es.bsc.mobile.types.Implementation getInternalImplementation() {
        return new es.bsc.mobile.types.Kernel(getCoreElementId(), getImplementationId(), getMethodName(), program, resultType, resultSize, workloadSize, localSize, offset);
    }

}
