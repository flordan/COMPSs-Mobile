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

/**
 * The DataManager is an utility class to store data and manage asynchronous
 * operations on them. To be notified about the end of the requested operations,
 * the developer has to implement an interface that handles the operation end
 * notifications.
 *
 *
 *
 * @author flordan
 */
public interface DataManager {

    /**
     * Checks if the DataManager knows about some data existence
     *
     * @param data Identifier of the data whose existence needs to be checked
     * @return {@literal true} if the node is aware of the data existence
     */
    public boolean checkDataExistence(String data);

    /**
     * Verifies that a data value exists in the system.
     *
     * @param data Identifier of the data whose existence needs to be checked
     * @param listener action to perform when the dataManager is notified of the
     * existence
     */
    public void requestDataExistence(String data, DataExistenceListener listener);

    /**
     * Queries the current state of the data to the DataManager.
     *
     * @param data Identifier of the data whose state needs to be fetched
     * @return current status of the data in the node
     */
    public DataStatus getDataStatus(String data);

    /**
     * Registers the existence of a data value.
     *
     * @param data Existing data
     */
    public void registerDataExistence(String data);

    /**
     *
     * @param receivedData
     * @param array
     */
    public void receivedObject(String receivedData, byte[] array);

    /**
     * Register an object value into the internal data stores and notifies to
     * the system that the value of the object is present in the local node.
     *
     * @param data Data Instance that identifies that object
     * @param value object value
     * @param listener action to perform when the dataManager has stored the
     * value
     */
    public void storeObject(String data, Object value, DataOperationListener listener);

    /**
     * The data manager fetches the data value as an object and notifies the
     * listener.
     *
     * @param sourceName identifier of the data to fetch
     * @param targetName identifier of the new dataId
     * @param listener action to perform when the dataManager has the notifier
     */
    public void retrieveObject(String sourceName, String targetName, DataOperationListener listener);

    /**
     *
     * @param receivedData
     * @param location
     */
    public void receivedFile(String receivedData, String location);

    /**
     * Registers a file into the internal data stores and notifies to the system
     * that a copy of that file exists in the node.
     *
     * @param data Data Instance that identifies the file
     * @param location location where the file is stored
     * @param listener action to perform when the dataManager has the notifier
     */
    public void storeFile(String data, String location, DataOperationListener listener);

    /**
     * The data manager fetches the data value as a file and notifies the
     * listener.
     *
     * @param sourceName identifier of the data to fetch
     * @param targetName identifier of the new dataId
     * @param listener action to perform when the dataManager has the notifier
     */
    public void retrieveFile(String sourceName, String targetName, DataOperationListener listener);

    /**
     * Ships a data value through the connection
     *
     * @param dataId identifier of the data to transfer
     * @param connection connection to transfer the data through
     */
    public void transferData(String dataId, Connection connection);

    /**
     * The data manager gets the size of a data value and notifies the
     * listener.
     *
     * @param dataId identifier of the data size
     * @param listener action to perform when the dataManager has the notifier
     */
    public void getSize(String dataId, DataOperationListener listener);

    public interface DataOperationListener {

        public void paused();

        public void setSize(long value);

        public void setValue(Class<?> type, Object value);

    }

    public interface DataExistenceListener {

        public void paused();

        public void exists();

        public void exists(Class<?> type, Object value);

    }

    public static class DataStatus {

        private final boolean existent;
        private final boolean present;
        private final boolean loaded;

        public DataStatus(boolean existence, boolean presence, boolean loaded) {
            this.existent = existence;
            this.present = presence;
            this.loaded = loaded;
        }

        public boolean isExistence() {
            return existent;
        }

        public boolean isPresence() {
            return present;
        }

        public boolean isLoaded() {
            return loaded;
        }
    }
}
