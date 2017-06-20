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

import es.bsc.comm.Connection;
import es.bsc.mobile.data.DataRegister;

/**
 *
 * @author flordan
 */
public class TransferData extends DataAction {

    private final DataRegister reg;
    private final Connection connection;

    public TransferData(DataRegister reg, Connection c) {
        this.reg = reg;
        this.connection = c;
    }

    @Override
    public void perform() {
        byte[] serialized;
        if (reg.isSerialized() && (serialized = reg.getArrayValue()) != null) {
            connection.sendDataArray(serialized);
        } else {
            String location = reg.getLocation();
            connection.sendDataFile(location);
        }
        connection.finishConnection();
    }

}
