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
package es.bsc.mobile.utils;

import es.bsc.comm.Node;
import es.bsc.mobile.types.comm.NodeRegistry;
import es.bsc.mobile.types.messages.runtime.DataExistenceRequest;
import es.bsc.mobile.types.messages.runtime.DataSourceRequest;
import es.bsc.mobile.utils.DataRegistry.DataRegister;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedList;

public class DataRegistry extends DistributedHashTable<DataRegister> {

    private static final HashMap<String, DataRegister> REGISTERS = new HashMap<>();

    public DataRegister registerData(String data, Node host) {
        DataRegister reg = obtainReg(data);
        reg.locations.add(host);
        return reg;
    }

    public void registerExistenceRequest(DataExistenceRequest der) {
        String data = der.getData();
        DataRegister reg = obtainReg(data);
        synchronized (reg) {
            if (reg.locations.isEmpty()) {
                reg.pendingExistence.add(der);
            }
        }
    }

    public void registerDataLocationsRequest(DataSourceRequest dsr) {
        String data = dsr.getData();
        DataRegister reg = obtainReg(data);
        synchronized (reg) {
            if (reg.locations.isEmpty()) {
                reg.pendingLocations.add(dsr);
            }
        }
    }

    private DataRegister obtainReg(String data) {
        DataRegister reg = REGISTERS.get(data);
        if (reg == null) {
            synchronized (REGISTERS) {
                reg = REGISTERS.get(data);
                if (reg == null) {
                    reg = new DataRegister(data);
                    REGISTERS.put(data, reg);
                    addHashValue(reg);
                }
            }
        }
        return reg;
    }

    public boolean checkExistence(String data) {
        DataRegister reg = REGISTERS.get(data);
        return reg != null && !reg.locations.isEmpty();
    }

    public DataRegister getRegister(String data) {
        return REGISTERS.get(data);
    }

    public HashSet<Node> getLocations(String data) {
        DataRegister reg = REGISTERS.get(data);
        if (reg != null) {
            return reg.getLocations();
        } else {
            return null;
        }
    }

    public String dump() {
        StringBuilder sb = new StringBuilder("Data Register:\n");
        for (java.util.Map.Entry<String, DataRegister> entry : REGISTERS.entrySet()) {
            sb.append("\t").append(entry.getKey()).append(":\n");
            sb.append("\t\tLocations:\n");
            DataRegister r = entry.getValue();
            for (Node n : r.locations) {
                sb.append("\t\t\t").append(n).append("\n");
            }
            sb.append("\t\tPending notifications:\n");
            for (DataExistenceRequest der : r.pendingExistence) {
                sb.append("\t\t\t").append(der.getSource()).append("\n");
            }
        }
        return sb.toString();
    }

    @Override
    public void addRegisters(LinkedList<DataRegister> toAdd) {
        for (DataRegister reg : toAdd) {
            HashSet<Node> locations = new HashSet<>();
            for (Node n : reg.locations) {
                locations.add(NodeRegistry.getRepresentive(n));
            }
            reg.locations = locations;
        }
    }

    @Override
    protected void removeRegisters(LinkedList<DataRegister> toRemove) {
        for (DataRegister reg : toRemove) {
            REGISTERS.remove(reg.getDataId());
        }
    }

    public static class DataRegister extends DistributedHashTable.Register {

        private HashSet<Node> locations;
        private HashSet<DataExistenceRequest> pendingExistence;
        private HashSet<DataSourceRequest> pendingLocations;

        public DataRegister() {
        }

        public DataRegister(String name) {
            super(name);
            locations = new HashSet<>();
            pendingExistence = new HashSet<>();
            pendingLocations = new HashSet<>();
        }

        public void addLocation(Node host) {
            locations.add(host);
        }

        public HashSet<Node> getLocations() {
            return locations;
        }

        public void removeLocation(Node host) {
            locations.remove(host);
        }

        public void addPendingExistence(DataExistenceRequest der) {
            pendingExistence.add(der);
        }

        public void addPendingLocations(DataSourceRequest dsr) {
            pendingLocations.add(dsr);
        }

        public HashSet<DataExistenceRequest> getPendingExistence() {
            return pendingExistence;
        }

        public HashSet<DataSourceRequest> getPendingLocations() {
            return pendingLocations;
        }

        public void clearPendingExistence() {
            pendingExistence.clear();
        }

        public void clearPendingLocations() {
            pendingLocations.clear();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeInt(locations.size());
            for (Node n : locations) {
                out.writeObject(n);
            }
            out.writeInt(pendingExistence.size());
            for (DataExistenceRequest n : pendingExistence) {
                out.writeObject(n);
            }
            out.writeInt(pendingLocations.size());
            for (DataSourceRequest n : pendingLocations) {
                out.writeObject(n);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            int size = in.readInt();
            locations = new HashSet<>();
            for (int i = 0; i < size; i++) {
                locations.add((Node) in.readObject());
            }
            size = in.readInt();
            pendingExistence = new HashSet<>();
            for (int i = 0; i < size; i++) {
                pendingExistence.add((DataExistenceRequest) in.readObject());
            }
            size = in.readInt();
            pendingLocations = new HashSet<>();
            for (int i = 0; i < size; i++) {
                pendingLocations.add((DataSourceRequest) in.readObject());
            }
        }

        @Override
        public LinkedList<Node> getAllNodes() {
            LinkedList<Node> nodes = new LinkedList<>();
            for (Node n : locations) {
                nodes.add(n);
            }
            for (DataExistenceRequest req : pendingExistence) {
                nodes.add(req.getSource());
            }
            for (DataSourceRequest req : pendingLocations) {
                nodes.add(req.getQuerier());
            }
            return nodes;
        }
    }
}
