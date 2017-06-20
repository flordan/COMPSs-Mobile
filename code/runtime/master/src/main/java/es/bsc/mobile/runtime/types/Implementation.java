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

import es.bsc.mobile.annotations.Parameter;

import java.io.Serializable;

public abstract class Implementation implements Serializable {

    private int coreElementId;
    private int implementationId;
    private final String methodName;

    public Implementation(int coreElementId, int implementationId, String methodName) {
        this.coreElementId = coreElementId;
        this.implementationId = implementationId;
        this.methodName = methodName;
    }

    public int getCoreElementId() {
        return coreElementId;
    }

    public void setCoreElementId(int coreElementId) {
        this.coreElementId = coreElementId;
    }

    public int getImplementationId() {
        return implementationId;
    }

    public void setImplementationId(int implementationId) {
        this.implementationId = implementationId;
    }

    public String getMethodName() {
        return methodName;
    }

    public abstract String completeSignature(String methodSignature);

    public static String getSignature(java.lang.reflect.Method method) {
        int numPars = method.getParameterAnnotations().length;
        Parameter.Type[] params = new Parameter.Type[numPars];
        for (int i = 0; i < numPars; i++) {
            params[i] = inferType(method.getParameterTypes()[i],
                    ((Parameter) method.getParameterAnnotations()[i][0]).type());
        }

        return getSignature(method.getName(), params);
    }

    public static String getSignature(String methodName, Parameter.Type[] types) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(methodName).append("(");
        int numPars = types.length;
        if (numPars > 0) {
            buffer.append(types[0]);
            for (int i = 1; i < numPars; i++) {
                buffer.append(",").append(types[i]);
            }
        }
        buffer.append(")");
        return buffer.toString();
    }

    /**
     * Infers the type of a parameter. If the parameter is annotated as a FILE
     * or a STRING, the type is taken from the annotation. If the annotation is
     * UNSPECIFIED, the type is taken from the formal type.
     *
     * @param formalType Formal type of the parameter
     * @param annotType Annotation type of the parameter
     * @return A String representing the type of the parameter
     */
    protected static Parameter.Type inferType(Class<?> formalType, Parameter.Type annotType) {
        Parameter.Type type;
        if (annotType.equals(Parameter.Type.UNSPECIFIED)) {
            if (formalType.isPrimitive()) {
                if (formalType.equals(boolean.class)) {
                    type = Parameter.Type.BOOLEAN;
                } else if (formalType.equals(char.class)) {
                    type = Parameter.Type.CHAR;
                } else if (formalType.equals(byte.class)) {
                    type = Parameter.Type.BYTE;
                } else if (formalType.equals(short.class)) {
                    type = Parameter.Type.SHORT;
                } else if (formalType.equals(int.class)) {
                    type = Parameter.Type.INT;
                } else if (formalType.equals(long.class)) {
                    type = Parameter.Type.LONG;
                } else if (formalType.equals(float.class)) {
                    type = Parameter.Type.FLOAT;
                } else {
                    //double.clas
                    type = Parameter.Type.DOUBLE;
                }
            } else if (formalType.isArray()) {
                type = null;
            } else {
                // Object
                return Parameter.Type.OBJECT;
            }
        } else {
            type = annotType;
        }
        return type;
    }

    public abstract es.bsc.mobile.types.Implementation getInternalImplementation();
}
