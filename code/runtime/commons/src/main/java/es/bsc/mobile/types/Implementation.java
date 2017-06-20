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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class Implementation implements Externalizable {

    private static final long serialVersionUID = 1L;

    private int coreElementId;
    private int implementationId;
    private String methodName;

    public Implementation() {
    }

    public Implementation(int coreElementId, int implementationId,
            String methodName) {
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

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(coreElementId);
        out.writeInt(implementationId);
        out.writeUTF(methodName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        coreElementId = in.readInt();
        implementationId = in.readInt();
        methodName = in.readUTF();
    }
}
