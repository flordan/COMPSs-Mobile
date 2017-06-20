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
package es.bsc.mobile.runtime.types.data.access;

import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.data.access.DaAccess;
import es.bsc.mobile.data.access.RAccess;

public class ReadAccess extends DataAccess {

    protected DataInstance readDataInstance;

    public ReadAccess() {
        super(Action.READ);
    }

    public ReadAccess(DataInstance data) {
        super(Action.READ);
        this.readDataInstance = data;
    }

    public DataInstance getReadDataInstance() {
        return readDataInstance;
    }

    public void setReadDataInstance(DataInstance data) {
        this.readDataInstance = data;
    }

    @Override
    public String toString() {
        return "Data Access for lecture on data " + readDataInstance;
    }

    @Override
    public Integer getDataID() {
        return readDataInstance.getDataId();
    }

    @Override
    public DaAccess prepareToOffload() {
        return new RAccess(readDataInstance.getRenaming());
    }

}
