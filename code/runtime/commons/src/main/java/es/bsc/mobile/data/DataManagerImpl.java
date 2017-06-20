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
import es.bsc.mobile.data.actions.LoadData;
import es.bsc.mobile.data.actions.SizeData;
import es.bsc.mobile.data.actions.TransferData;

import es.bsc.mobile.data.actions.DataAction;
import es.bsc.mobile.data.actions.OwnedData;
import es.bsc.mobile.data.actions.RequestData;
import es.bsc.mobile.types.Operation;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The DataManager class is an utility class to store data and manage
 * asynchronous operations on them. To be notified about the end of the
 * requested operations, the developer has to implement an interface that
 * handles the operation end notifications.
 *
 *
 *
 * @author flordan
 */
public class DataManagerImpl implements DataManager {

    /**
     * The DMBackBone interface encapsulates all the operations that the data
     * requires to fulfill its duty. Currently the DM requires 3 operations
     * <ul>
     * <li>announceDataPresence: </li>
     * <li>requestDataAsFile: </li>
     * <li>requestDataAsObject: </li>
     * </ul>
     *
     */
    public interface DMBackBone {

        /**
         * Notifies to the whole system that a replica of a data instance is
         * hosted by the local node
         *
         * @param data Id of the data hosted in the local node
         * @return {@literal true} if the node is aware of the data existence
         */
        public boolean askDataExistence(String data);

        /**
         * Notifies to the whole system that a replica of a data instance is
         * hosted by the local node
         *
         * @param data Id of the data hosted in the local node
         */
        public void announceDataPresence(String data);

        /**
         * Requests the transmission of a data instance to the local node and
         * store it as an object.
         *
         * @param dataId data to be obtained
         */
        public void requestDataAsObject(String dataId);

        /**
         * Requests the transmission of a data instance to the local node and
         * store it as a file.
         *
         * @param dataId data to be obtained
         * @param targetLocation location where to leave the value
         */
        public void requestDataAsFile(String dataId, String targetLocation);
    }

    private static final Logger LOGGER = LogManager.getLogger("Runtime.DataSharing");
    private static final boolean LOG_ENABLED = LOGGER.isInfoEnabled();

    private static final String REGISTERING = "Registering data ";
    private static final String EXISTENCE = "existence";

    private static final String END = ".";

    private final DMBackBone backbone;
    private final TreeSet<String> existingData = new TreeSet<>();
    private final HashMap<String, LinkedList<Operation>> requestedData;
    private final DataStore store;

    private LinkedList<DataAction> dataActions = new LinkedList<>();
    private LinkedList<DataAction> dataActionsBackup = new LinkedList<>();

    private final TreeMap<String, LinkedList<DataExistenceListener>> pendingExistenceRequests = new TreeMap<>();

    private boolean keepGoing;
    private boolean waiting = false;

    /**
     * Constructs a new data Manager
     *
     * @param backbone class that will handle
     */
    public DataManagerImpl(DMBackBone backbone) {
        this.backbone = backbone;
        store = new DataStore(this);
        requestedData = new HashMap<>();
        keepGoing = true;
        (new Thread() {
            @Override
            public void run() {
                setName("Data Manager");
                processDataActions();
            }
        }).start();
    }

    @Override
    public void registerDataExistence(String data) {
        registerDataExistence(data, false, null, null);
    }

    private void registerDataExistence(String data, boolean knownValue, Class<?> type, Object value) {
        if (LOG_ENABLED) {
            LOGGER.info(REGISTERING + data + EXISTENCE + END);
        }
        synchronized (pendingExistenceRequests) {
            existingData.add(data);
            LinkedList<DataExistenceListener> pendingReqs = pendingExistenceRequests.remove(data);
            if (pendingReqs != null) {
                for (DataExistenceListener listener : pendingReqs) {
                    if (knownValue) {
                        listener.exists(type, value);
                    } else {
                        listener.exists();
                    }
                }
            }
        }
    }

    @Override
    public boolean checkDataExistence(String data) {
        return existingData.contains(data);
    }

    @Override
    public void requestDataExistence(String data, DataExistenceListener listener) {
        DataRegister reg = store.query(data);
        Object value;
        if (reg != null && reg.isLoaded() && ((value = reg.getValue()) != null)) {
            listener.exists(value.getClass(), value);
        } else {
            synchronized (pendingExistenceRequests) {
                if (!existingData.contains(data)) {
                    if (!backbone.askDataExistence(data)) {
                        LinkedList<DataExistenceListener> pendingReqs = pendingExistenceRequests.get(data);
                        if (pendingReqs == null) {
                            pendingReqs = new LinkedList<>();
                            pendingExistenceRequests.put(data, pendingReqs);
                        }
                        pendingReqs.add(listener);
                    } else {
                        listener.exists();
                    }
                } else {
                    listener.exists();
                }
            }
        }
    }

    @Override
    public DataStatus getDataStatus(String data) {
        boolean existence = existingData.contains(data);
        boolean presence = false;
        boolean loaded = false;
        if (existence) {
            DataRegister dReg = store.query(data);
            if (dReg != null) {
                presence = dReg.isPresent();
                loaded = dReg.isLoaded();
            }
        }
        return new DataStatus(existence, presence, loaded);
    }

    @Override
    public void storeObject(String dataId, Object value, DataOperationListener listener) {
        //replacement Cache
        Operation op = new Operation(true, dataId, dataId, listener);
        store.store(dataId, value, op);
        registerDataExistence(dataId, true, value.getClass(), value);
        addAction(new OwnedData(dataId, backbone));
    }

    @Override
    public void storeFile(String dataId, String location, DataOperationListener listener) {
        Operation op = new Operation(false, dataId, dataId, listener);
        store.store(dataId, location, op);
        registerDataExistence(dataId, true, String.class, location);
        addAction(new OwnedData(dataId, backbone));
    }

    @Override
    public void transferData(String dataId, Connection c) {
        DataRegister reg = store.query(dataId);
        if (reg.isOnFile() || reg.isSerialized()) {
            readyToTransfer(reg, c);
        } else {
            store.prepareToTransfer(reg, c);
        }
    }

    @Override
    public void receivedObject(String dataId, byte[] value) {
        LinkedList<Operation> operations;
        synchronized (requestedData) {
            operations = requestedData.remove(dataId);
        }
        if (operations != null) {
            DataRegister or = store.store(dataId, value);
            long size = value.length;
            for (Operation op : operations) {
                if (op.getRename().equals(op.getSource())) {
                    store.loadRegister(or, op);
                } else {
                    store.replicateRegister(or, op);
                }
                addAction(new SizeData(size, op));
            }
            addAction(new OwnedData(dataId, backbone));
        }
    }

    @Override
    public void receivedFile(String dataId, String location) {
        LinkedList<Operation> operations;
        synchronized (requestedData) {
            operations = requestedData.remove(dataId);
        }
        if (operations != null) {
            DataRegister dr = store.store(dataId, location);
            long size = dr.getSize();
            for (Operation op : operations) {
                if (op.getRename().equals(op.getSource())) {
                    addAction(new LoadData(String.class, op.getSource(), op));
                } else {
                    store.replicateRegister(dr, op);
                }
                addAction(new SizeData(size, op));
            }
            addAction(new OwnedData(dataId, backbone));
        }
    }

    @Override
    public void getSize(String dataId, DataOperationListener listener) {
        Operation operation = new Operation(false, dataId, dataId, listener);
        DataRegister reg = store.query(dataId);
        store.sizeRegister(reg, operation);
    }

    @Override
    public void retrieveFile(String dataId, String target, DataOperationListener listener) {
        Operation operation = new Operation(false, dataId, target, listener);
        //We have received a file
        //Manage a file reception

        DataRegister reg = store.query(dataId);
        if (reg == null) {
            // Data does not exists in the node
            listener.paused();
            addAction(new RequestData(operation, backbone, this));
        } else // Data is already in the node
        {
            if (dataId.compareTo(target) == 0) {
                listener.setValue(String.class, reg.getLocation());
                listener.setSize(reg.getSize());
            } else {
                listener.paused();
                store.replicateRegister(reg, operation);
            }
        }
    }

    @Override
    public void retrieveObject(String dataId, String target, DataOperationListener listener) {
        Operation operation = new Operation(true, dataId, target, listener);
        DataRegister reg = store.query(dataId);
        if (reg == null) {
            // Data does not exists in the node
            listener.paused();
            addAction(new RequestData(operation, backbone, this));
        } else // Data is already in the node
        {
            if (dataId.compareTo(target) == 0) {
                Object value;
                if (reg.isLoaded() && (value = reg.getValue()) != null) {
                    listener.setValue(value.getClass(), value);
                } else {
                    listener.paused();
                    store.loadRegister(reg, operation);
                }
                store.sizeRegister(reg, operation);
            } else {
                listener.paused();
                store.replicateRegister(reg, operation);
            }
        }
    }

    private void addAction(DataAction action) {
        synchronized (this) {
            dataActions.add(action);
            if (waiting) {
                this.notify();
            }
        }
    }

    private void processDataActions() {
        while (keepGoing) {
            LinkedList<DataAction> privateActions;
            synchronized (this) {
                if (dataActions.isEmpty()) {
                    waiting = true;
                    try {
                        this.wait();
                    } catch (InterruptedException ie) {
                        //Do nothing
                    }
                }
                waiting = false;
                privateActions = dataActions;
                dataActions = dataActionsBackup;
                dataActionsBackup = privateActions;
            }
            for (DataAction action : privateActions) {
                action.perform();
            }
            privateActions.clear();
        }
    }

    public String getTempDataDir() {
        return store.getTempDataDir();
    }

    public boolean registerDataRequest(String dataId, Operation op) {
        synchronized (requestedData) {
            LinkedList<Operation> pending = requestedData.get(dataId);
            if (pending == null) {
                pending = new LinkedList<>();
                requestedData.put(dataId, pending);
                pending.add(op);
                return true;
            } else {
                pending.add(op);
                return false;
            }
        }
    }

    public void retrivedValue(Operation op, Class<?> type, Object value) {
        LoadData ald = new LoadData(type, value, op);
        addAction(ald);
    }

    public void obtainedSize(Operation op, long size) {
        SizeData asd = new SizeData(size, op);
        addAction(asd);
    }

    public void readyToTransfer(DataRegister reg, Connection c) {
        TransferData atd = new TransferData(reg, c);
        addAction(atd);
    }

}
