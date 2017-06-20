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
import es.bsc.mobile.types.comm.NodeAndHash;
import es.bsc.mobile.utils.DistributedHashTable.Register;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;

public abstract class DistributedHashTable<T extends Register> {

    private final TreeSet<HashSet<T>> hashIndex = new TreeSet<>();

    protected void addHashValue(Register reg) {
        short hash = NodeAndHash.getHash(reg.dataId);
        HashSet hs = new HashSet(hash);
        HashSet registeredHS = hashIndex.floor(hs);
        if (hs.equals(registeredHS)) {
            hs = registeredHS;
        } else {
            hs.registers = new LinkedList();
            hashIndex.add(hs);
        }
        hs.addRegister(reg);
    }

    public LinkedList<T> retrieveBiggerThan(short hash) {
        LinkedList<T> result = new LinkedList<>();
        Iterator<HashSet<T>> bigger = hashIndex.tailSet(new HashSet(hash)).iterator();
        while (bigger.hasNext()) {
            HashSet hs = bigger.next();
            result.addAll(hs.registers);
            removeRegisters(hs.registers);
            bigger.remove();
        }
        return result;
    }

    public abstract void addRegisters(LinkedList<T> registers);

    protected abstract void removeRegisters(LinkedList<T> registers);

    private static class HashSet<T extends Register> implements Comparable<HashSet> {

        private final short id;
        private LinkedList<T> registers;

        public HashSet(short hash) {
            this.id = hash;
        }

        public void addRegister(T reg) {
            registers.add(reg);
        }

        public void setRegisters(LinkedList<T> regs) {
            this.registers = regs;
        }

        @Override
        public int compareTo(HashSet o) {
            return id - o.id;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof HashSet && ((HashSet) o).id == id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    public abstract static class Register implements Externalizable {

        private String dataId;

        public Register() {
        }

        public Register(String name) {
            dataId = name;
        }

        public String getDataId() {
            return dataId;
        }

        public abstract LinkedList<Node> getAllNodes();

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(dataId);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dataId = in.readUTF();
        }
    }
}
