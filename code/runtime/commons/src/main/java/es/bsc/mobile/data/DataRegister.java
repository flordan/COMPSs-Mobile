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
package es.bsc.mobile.data;

import es.bsc.mobile.data.operations.DataOp;
import java.io.File;
import java.util.LinkedList;

public class DataRegister implements Comparable<DataRegister> {

    private static final byte LOADED = 0x04;
    private static final byte SERIALIZED = 0x02;
    private static final byte ON_FILE = 0x01;

    private static final byte NOT_LOADED_MASK = ~LOADED;
    private static final byte NOT_SERIALIZED_MASK = ~SERIALIZED;
    private static final byte NOT_ON_FILE_MASK = ~ON_FILE;

    private final String name;
    private byte state = 0x00;

    private Object value;
    private byte[] serialized;
    private String location;

    private LinkedList<DataOp> pendingForSerialization = null;
    private LinkedList<DataOp> pendingForDeserialization = null;

    public DataRegister(String name, Object value) {
        this.name = name;
        this.value = value;
        this.pendingForSerialization = new LinkedList();
        state = LOADED;

    }

    public DataRegister(String name, byte[] serialized) {
        this.name = name;
        this.serialized = serialized;
        this.pendingForDeserialization = new LinkedList();
        state = SERIALIZED;
    }

    public DataRegister(String name, String location) {
        this.name = name;
        this.location = location;
        this.pendingForDeserialization = new LinkedList();
        state = ON_FILE;
    }

    public String getName() {
        return name;
    }

    public boolean isPresent() {
        return state != 0x00;
    }

    public void addValue(Object o) {
        value = o;
        state |= LOADED;
    }

    public boolean isLoaded() {
        return (state & LOADED) != 0x00;
    }

    public Object getValue() {
        return value;
    }

    public void removeValue() {
        this.pendingForDeserialization = new LinkedList<>();
        state &= NOT_LOADED_MASK;
        value = null;
    }

    public void addArrayValue(byte[] array) {
        serialized = array;
        state |= SERIALIZED;
    }

    public boolean isSerialized() {
        return (state & SERIALIZED) != 0x00;
    }

    public byte[] getArrayValue() {
        return serialized;
    }

    public synchronized void removeArrayValue() {
        state &= NOT_SERIALIZED_MASK;
        if (!isOnFile()) {
            this.pendingForSerialization = new LinkedList<>();
        }
        serialized = null;
    }

    public void addFile(String location) {
        this.location = location;
        state |= ON_FILE;
    }

    public boolean isOnFile() {
        return (state & ON_FILE) != 0x00;
    }

    public String getLocation() {
        return location;
    }

    public void removeFile() {
        state &= NOT_ON_FILE_MASK;
        if (!isSerialized()) {
            this.pendingForSerialization = new LinkedList<>();
        }
        this.location = null;
    }

    public long getSize() {
        if (isSerialized()) {
            byte[] array = this.serialized;
            if (array != null) {
                return array.length;
            }
        }
        String path = location;
        return (new File(path)).length();
    }

    @Override
    public String toString() {
        return name + ": " + (isLoaded() ? "L" : "") + (isSerialized() ? "S" : "") + (isOnFile() ? "F" : "");
    }

    @Override
    public int compareTo(DataRegister o) {
        return name.compareTo(((DataRegister) o).getName());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof DataRegister) {
            return name.equals(((DataRegister) o).getName());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public void deserialized(Object o) {
        addValue(o);
        LinkedList<DataOp> pendingOps;
        synchronized (this) {
            pendingOps = pendingForDeserialization;
            pendingForDeserialization = null;
        }
        for (DataOp op : pendingOps) {
            op.finished(this);
        }
    }

    public void serialized(byte[] array) {
        addArrayValue(array);
        LinkedList<DataOp> pendingOps;
        synchronized (this) {
            pendingOps = pendingForSerialization;
            pendingForSerialization = null;
        }
        for (DataOp op : pendingOps) {
            op.finished(this);
        }
    }

    public void serialized(String location) {
        addFile(location);
        LinkedList<DataOp> pendingOps;
        synchronized (this) {
            pendingOps = pendingForSerialization;
            pendingForSerialization = null;
        }
        for (DataOp op : pendingOps) {
            op.finished(this);
        }
    }

    public void addOperationForDeserialization(DataOp op) {
        synchronized (this) {
            pendingForDeserialization.add(op);
        }
    }

    public void addOperationForSerialization(DataOp op) {
        synchronized (this) {
            pendingForSerialization.add(op);
        }
    }

}
