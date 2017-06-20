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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class Method extends Implementation {

    protected String declaringClass;
    protected Constraints constraints;

    public Method() {
        super();
    }

    public Method(int coreElementId, int implementationtId, String methodName,
            String declaringClass, Constraints constraints) {
        super(coreElementId, implementationtId, methodName);
        this.constraints = constraints;
        this.declaringClass = declaringClass;
    }

    public String getDeclaringClass() {
        return declaringClass;
    }

    public Constraints getConstraints() {
        return constraints;
    }

    @Override
    public String toString() {
        return super.toString() + ": " + declaringClass + "." + getMethodName();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(declaringClass);
        out.writeObject(constraints);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        declaringClass = in.readUTF();
        constraints = (Constraints) in.readObject();
    }
}
