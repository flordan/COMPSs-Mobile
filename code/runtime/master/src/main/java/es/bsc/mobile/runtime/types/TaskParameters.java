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

import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.utils.CoreManager;
import java.io.Serializable;

public class TaskParameters implements Serializable {

    private final int coreId;
    private final String methodName;
    private final boolean target;
    private final boolean result;
    private final Parameter[] parameters;

    public TaskParameters(String declaringClass, String methodName,
            boolean hasTarget, Parameter[] parameters) {
        this.methodName = methodName;
        this.target = hasTarget;
        if (parameters.length == 0) {
            this.result = false;
        } else {
            Parameter lastParam = parameters[parameters.length - 1];
            this.result = (lastParam.getDirection() == Direction.OUT && lastParam.getType() == Type.OBJECT);
        }
        String signature = Method.getSignature(declaringClass, methodName, hasTarget, this.result, parameters);
        this.coreId = CoreManager.getCoreId(signature);
        this.parameters = parameters;
    }

    public int getCoreId() {
        return coreId;
    }

    public String getMethodName() {
        return methodName;
    }

    public boolean hasTarget() {
        return target;
    }

    public boolean hasReturn() {
        return result;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("\t-Core Id: ").append(coreId).append("\n");
        sb.append("\t-Method name: ").append(methodName).append("\n");
        sb.append("\t-Has Target: ").append(target).append("\n");
        sb.append("\t-Has Return: ").append(result).append("\n");
        sb.append("\t-Parameters: ").append("\n");
        for (Parameter parameter : parameters) {
            sb.append("\t\t - ").append(parameter).append("\n");
        }
        return sb.toString();
    }

}
