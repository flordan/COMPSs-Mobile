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
package es.bsc.mobile.runtime.types.data;

import java.io.IOException;

public class RegisteredFile<T> extends RegisteredData {

    public RegisteredFile() {
        super();
    }

    @Override
    public RegisteredData genInOutRemote() {
        return new RegisteredFile();
    }

    @Override
    public RegisteredData genInOutLocal() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void saveForTransfer() throws IOException {
        //Nothing to do. It's a  file so there's no need to do any operation to
        //prepare the file to be transferred
    }

    @Override
    public boolean isSavedForTransfer() {
        //Since it is already stored in the device disk, there is no need to do
        //any operation to prepare it to be transferred; therefore, it is ready
        //for being transferred.
        return true;
    }
}
