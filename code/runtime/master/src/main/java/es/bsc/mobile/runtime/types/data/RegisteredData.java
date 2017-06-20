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

public abstract class RegisteredData {

    public enum DataLocation {

        LOCAL, REMOTE
    }

    /*
     * UNDEFINED =0; 0x00 LOCAL = 1; 0x01 REMOTE = 2; 0x10 LOCALREMOTE = 3; 0x11
     */
    protected int location = 0;
    protected static final int LOCAL = 1;
    protected static final int REMOTE = 2;
    protected static final int LOCALREMOTE = 3;

    public void addLocation(DataLocation loc) {
        switch (loc) {
            case LOCAL:
                location = location | LOCAL;
                break;
            default:
                location = location | REMOTE;
        }
    }

    public void removeLocation(DataLocation loc) {
        if (loc == DataLocation.LOCAL) {
            location = location & REMOTE;
        } else {
            location = location & LOCAL;
        }
    }

    public boolean isLocal() {
        return (this.location & LOCAL) == 1;
    }

    public abstract RegisteredData genInOutRemote();

    public abstract RegisteredData genInOutLocal();

    public String dump() {
        StringBuilder sb = new StringBuilder();
        switch (location) {
            case LOCAL:
                sb.append("(L) ");
                break;
            case REMOTE:
                sb.append("(R) ");
                break;
            case LOCALREMOTE:
                sb.append("(LR)");
                break;
            default:
                sb.append("()  ");
        }
        return sb.toString();

    }

    public abstract void saveForTransfer() throws IOException;

    public abstract boolean isSavedForTransfer() throws IOException;
}
