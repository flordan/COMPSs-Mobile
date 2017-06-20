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
package es.bsc.mobile.types;

import es.bsc.mobile.utils.Expression;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Kernel extends Implementation {

    private String program;
    private Expression[] workloadExpressions;
    private Expression[] localSizeExpressions;
    private Expression[] offsetExpressions;
    private Class<?> resultType;
    private Expression[] resultSizeExpressions;

    public Kernel() {
        super();
    }

    public Kernel(int coreElementId, int implementationtId, String methodName, String program, Class<?> resultType, String[] resultSizeExpression, String[] workloadExpressions, String[] localSizeExpressions, String[] offsetExpressions) {
        super(coreElementId, implementationtId, methodName);
        this.program = program;
        this.workloadExpressions = new Expression[workloadExpressions.length];
        for (int i = 0; i < workloadExpressions.length; i++) {
            this.workloadExpressions[i] = Expression.newExpression(workloadExpressions[i]);
        }
        this.localSizeExpressions = new Expression[localSizeExpressions.length];
        for (int i = 0; i < localSizeExpressions.length; i++) {
            this.localSizeExpressions[i] = Expression.newExpression(localSizeExpressions[i]);
        }
        this.offsetExpressions = new Expression[offsetExpressions.length];
        for (int i = 0; i < offsetExpressions.length; i++) {
            this.offsetExpressions[i] = Expression.newExpression(offsetExpressions[i]);
        }
        this.resultSizeExpressions = new Expression[resultSizeExpression.length];
        for (int i = 0; i < resultSizeExpression.length; i++) {
            this.resultSizeExpressions[i] = Expression.newExpression(resultSizeExpression[i]);
        }
        this.resultType = resultType;
    }

    public String getProgram() {
        return program;
    }

    @Override
    public String toString() {
        return super.toString() + ": " + program + "." + getMethodName();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(program);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        program = in.readUTF();
    }

    public Expression[] getWorkloadExpressions() {
        return workloadExpressions;
    }

    public Expression[] getLocalSizeExpressions() {
        return localSizeExpressions;
    }

    public Expression[] getOffsetExpressions() {
        return offsetExpressions;
    }

    public Class<?> getResultType() {
        return resultType;
    }

    public Expression[] getResultSizeExpressions() {
        return resultSizeExpressions;
    }
}
