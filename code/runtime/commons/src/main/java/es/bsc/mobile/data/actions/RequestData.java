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
package es.bsc.mobile.data.actions;

import es.bsc.mobile.data.DataManagerImpl;
import es.bsc.mobile.types.Operation;
import es.bsc.mobile.data.DataManagerImpl.DMBackBone;

public class RequestData extends DataAction {

    private final DMBackBone backbone;
    private final Operation op;
    private final DataManagerImpl dataManager;

    public RequestData(Operation op, DMBackBone user, DataManagerImpl dm) {
        this.backbone = user;
        this.op = op;
        dataManager = dm;
    }

    @Override
    public String toString() {
        return "Requesting operation " + op;
    }

    @Override
    public void perform() {
        String dataIn = op.getSource();
        if (dataManager.registerDataRequest(dataIn, op)) {
            if (op.isObject()) {
                backbone.requestDataAsObject(dataIn);
            } else {
                backbone.requestDataAsFile(dataIn, dataManager.getTempDataDir() + op.getRename());
            }
        }
    }

}
