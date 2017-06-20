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

import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.types.Constraints;

public class Method extends Implementation {

    private final String declaringClass;
    private final Constraints constraints;

    public Method(int coreElementId, int implementationtId, String methodName,
            String declaringClass, Constraints constraints) {
        super(coreElementId, implementationtId, methodName);
        this.declaringClass = declaringClass;
        this.constraints = constraints;
    }

    public String getDeclaringClass() {
        return declaringClass;
    }

    @Override
    public String completeSignature(String methodSignature) {
        return declaringClass + "." + methodSignature;
    }

    public static String getSignature(String declaringClass, String methodName,
            boolean hasTarget, boolean hasReturn, Parameter[] parameters) {
        int paramCount = parameters.length;
        if (hasTarget) {
            paramCount--;
        }
        if (hasReturn) {
            paramCount--;
        }
        Type[] types = new Type[paramCount];
        for (int i = 0; i < paramCount; i++) {
            types[i] = parameters[i].getType();
        }
        String methodSignature = Implementation.getSignature(methodName, types);
        return declaringClass + "." + methodSignature;
    }

    public Constraints getConstraints() {
        return constraints;
    }

    @Override
    public es.bsc.mobile.types.Implementation getInternalImplementation() {
        return new es.bsc.mobile.types.Method(getCoreElementId(), getImplementationId(), getMethodName(), declaringClass, constraints);
    }
}
