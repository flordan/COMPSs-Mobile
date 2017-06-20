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

import es.bsc.mobile.utils.Serializer;
import java.io.IOException;

public class RegisteredObject<T> extends RegisteredData {

    private boolean waiting;
    private T object;
    private byte[] serialized;
    private boolean keepObject;

    public RegisteredObject() {
        super();
        object = null;
        serialized = null;
        keepObject = true;
    }

    public RegisteredObject(T o) {
        super();
        object = o;
        keepObject = true;
    }

    public T getObject() {
        return object;
    }

    public byte[] getSerialized() {
        return serialized;
    }

    public void setObject(T object) {
        this.object = object;
    }

    public void setSerialized(byte[] serialized) {
        this.serialized = serialized;
    }

    public T getValue() throws InterruptedException {
        if (!isLocal()) {
            synchronized (this) {
                if (!isLocal()) {
                    waiting = true;
                    this.wait();
                    waiting = false;
                }
            }
        }
        return this.object;
    }

    public void setValue(T o) {
        this.object = o;
        synchronized (this) {
            this.addLocation(DataLocation.LOCAL);
            if (waiting) {
                waiting = false;
                this.notifyAll();
            }
        }
    }

    @Override
    public RegisteredData genInOutRemote() {
        keepObject = false;
        if (isSavedForTransfer()) {
            object = null;
        }
        return new RegisteredObject();
    }

    @Override
    public RegisteredData genInOutLocal() {
        RegisteredObject ro = new RegisteredObject();
        ro.object = object;
        if (this.isLocal()) {
            ro.addLocation(DataLocation.LOCAL);
        }
        keepObject = false;
        object = null;
        removeLocation(DataLocation.LOCAL);
        return ro;
    }

    @Override
    public void saveForTransfer() throws IOException {
        serialized = Serializer.serialize(object);
        if (!keepObject) {
            object = null;
        }
    }

    @Override
    public boolean isSavedForTransfer() {
        return serialized != null;
    }

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
        sb.append("Object " + this.object + " serialized:" + this.serialized);
        return sb.toString();

    }
}
