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
package es.bsc.mobile.data.access;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public abstract class DaAccess implements Externalizable {

    private static final long serialVersionUID = 1L;

    public enum Action {

        READ, WRITE, UPDATE
    }

    protected Action action;

    public DaAccess() {
    }

    public DaAccess(Action action) {
        this.action = action;
    }

    public Action getAction() {
        return this.action;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(action.ordinal());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        action = Action.values()[in.readInt()];
    }

}
