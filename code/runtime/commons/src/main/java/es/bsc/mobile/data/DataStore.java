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

import es.bsc.comm.Connection;
import es.bsc.mobile.Configuration;
import es.bsc.mobile.data.operations.DataOp;
import es.bsc.mobile.types.Operation;
import es.bsc.mobile.utils.SerializationManager;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class DataStore {

    private final DataManagerImpl user;
    private final String cacheDir;
    private final HashMap<String, DataRegister> registers = new HashMap<>();
    private final LinkedBlockingQueue<DataRegister> toFile = new LinkedBlockingQueue<>();
    private final SerializationManager sm;

    public DataStore(DataManagerImpl user) {
        this.user = user;
        this.cacheDir = Configuration.getDataDir();
        sm = new SerializationManager(2);
    }

    public void store(String dataId, String location, Operation op) {
        DataRegister r = new DataRegister(dataId, location);
        long size = r.getSize();
        user.obtainedSize(op, size);
        registers.put(dataId, r);
    }

    public void store(String dataId, Object o, Operation op) {
        DataRegister reg = new DataRegister(dataId, o);
        sm.requestSerialization(false, reg, new SizeData(op));
        sm.writeToFile(reg, cacheDir);
        registers.put(dataId, reg);
    }

    public DataRegister store(String dataId, byte[] serialized) {
        DataRegister r = new DataRegister(dataId, serialized);
        sm.writeToFile(r, cacheDir);
        registers.put(dataId, r);
        return r;
    }

    public DataRegister store(String dataId, String location) {
        DataRegister r = new DataRegister(dataId, location);
        registers.put(dataId, r);
        return r;
    }

    public DataRegister dummyRegister(String dataId, byte[] serialized) {
        return new DataRegister(dataId, serialized);
    }

    public DataRegister dummyRegister(String dataId, String location) {
        return new DataRegister(dataId, location);
    }

    public DataRegister query(String dataId) {
        DataRegister reg = registers.get(dataId);
        if (reg == null && (new File(cacheDir + dataId)).exists()) {
            reg = new DataRegister(dataId, cacheDir + dataId);
        }
        return reg;
    }

    public void prepareToTransfer(DataRegister reg, Connection c) {
        sm.requestSerialization(true, reg, new TransferData(c));
    }

    public void loadRegister(DataRegister reg, Operation op) {
        Object value;
        if (reg.isLoaded() && (value = reg.getValue()) != null) {
            user.retrivedValue(op, value.getClass(), value);
        } else {
            sm.requestDeserialization(true, reg, new LoadObject(op));
        }
    }

    public void replicateRegister(DataRegister reg, Operation op) {
        if (op.isObject()) {
            DataRegister dummy = null;
            if (reg.isOnFile()) {
                dummy = new DataRegister(op.getRename(), reg.getLocation());
                sm.requestDeserialization(true, dummy, new LoadObject(op));
            } else if (reg.isSerialized()) {
                dummy = new DataRegister(op.getRename(), reg.getArrayValue());
                sm.requestDeserialization(true, dummy, new LoadObject(op));
            } else {
                dummy = new DataRegister(op.getRename(), reg.getValue());
                sm.requestSerialization(true, dummy, new ReplicateObject(op));
            }
        } else {
            sm.replicateFile(true, reg, cacheDir + op.getRename(), new LoadFile(op));
        }
    }

    public void sizeRegister(DataRegister reg, Operation op) {
        if (reg.isOnFile() || reg.isSerialized()) {
            long size = reg.getSize();
            user.obtainedSize(op, size);
        } else {
            sm.requestSerialization(false, reg, new SizeData(op));
        }
    }

    public void flushToFile(DataRegister reg) throws IOException {
        toFile.offer(reg);
    }

    public void removeData(String dataId) {
        registers.remove(dataId);
    }

    public String dump(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("Cache content:").append("\n");
        for (DataRegister register : registers.values()) {
            sb.append(prefix).append("\t").append(register).append("\n");
        }
        return sb.toString();
    }

    public final String getTempDataDir() {
        return cacheDir;
    }

    private class LoadObject extends DataOp {

        private final Operation op;

        public LoadObject(Operation op) {
            this.op = op;
        }

        @Override
        public void finished(DataRegister reg) {
            Object o = reg.getValue();
            user.retrivedValue(op, o.getClass(), o);
        }
    }

    private class LoadFile extends DataOp {

        private final Operation op;

        public LoadFile(Operation op) {
            this.op = op;
        }

        @Override
        public void finished(DataRegister reg) {
            String location = reg.getLocation();
            user.retrivedValue(op, String.class, location);
        }
    }

    private class TransferData extends DataOp {

        private final Connection c;

        public TransferData(Connection c) {
            this.c = c;
        }

        @Override
        public void finished(DataRegister reg) {
            Object o = reg.getValue();
            user.readyToTransfer(reg, c);
        }
    }

    private class SizeData extends DataOp {

        private final Operation operation;

        public SizeData(Operation op) {
            this.operation = op;
        }

        @Override
        public void finished(DataRegister reg) {
            Object o = reg.getValue();
            long size = reg.getSize();
            user.obtainedSize(operation, size);
        }
    }

    private class ReplicateObject extends DataOp {

        private final Operation operation;

        public ReplicateObject(Operation op) {
            this.operation = op;
        }

        @Override
        public void finished(DataRegister reg) {
            reg.removeValue();
            sm.requestDeserialization(true, reg, new LoadObject(operation));
        }
    }
}
