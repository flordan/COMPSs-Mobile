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
package es.bsc.mobile.runtime.types.data.parameter;

import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.runtime.types.data.access.DataAccess;
import java.io.Serializable;

public class RegisteredParameter extends Parameter implements Serializable {

    private DataAccess dAccess;

    public RegisteredParameter(Type type, Direction direction, DataAccess dAccess) {
        super(type, direction);
        this.dAccess = dAccess;
    }

    public DataAccess getDAccess() {
        return dAccess;
    }

    public void setDAccess(DataAccess dAccess) {
        this.dAccess = dAccess;
    }

    @Override
    public String toString() {
        return dAccess + " " + getType() + " " + getDirection();
    }

}
