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
package es.bsc.mobile.node;

import es.bsc.comm.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.MessageHandler;
import es.bsc.comm.Node;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.data.DataManager;
import es.bsc.mobile.data.DataManager.DataOperationListener;
import es.bsc.mobile.data.DataManagerImpl;
import es.bsc.mobile.types.Job;
import es.bsc.mobile.types.JobProfile;

import es.bsc.mobile.types.comm.NodeRegistry;
import es.bsc.mobile.types.messages.Message;
import es.bsc.mobile.types.messages.runtime.DataCreationNotification;
import es.bsc.mobile.types.messages.runtime.DataExistenceNotification;
import es.bsc.mobile.types.messages.runtime.DataExistenceRequest;
import es.bsc.mobile.types.messages.runtime.DataSourceRequest;
import es.bsc.mobile.types.messages.runtime.DataSourceResponse;
import es.bsc.mobile.utils.DataRegistry;
import es.bsc.mobile.utils.DataRegistry.DataRegister;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import es.bsc.mobile.data.DataManagerImpl.DMBackBone;
import es.bsc.mobile.data.DataManager.DataStatus;

public abstract class RuntimeNode extends P2PNode<DataRegistry> implements MessageHandler, DMBackBone {

    private static final Logger LOGGER = LogManager.getLogger("Runtime.DataSharing");

    private final DataManager dataManager;
    private final HashMap<String, String> dataDestination = new HashMap<>();

    public RuntimeNode(Node me) {
        super(me, new DataRegistry());
        this.dataManager = new DataManagerImpl(this);
    }

    @Override
    public void commandReceived(Connection c, Transfer t) {
        Message m = (Message) t.getObject();
        m.handleMessage(c, this);
    }

    /**
     * Queries the current state of the data to the DataManager.
     *
     * @param data Identifier of the data whose state needs to be fetched
     * @return current status of the data in the node
     */
    public DataStatus getDataStatus(String data) {
        return dataManager.getDataStatus(data);
    }

    public void requestDataExistence(String data, DataManager.DataExistenceListener listener) {
        dataManager.requestDataExistence(data, listener);
    }

    /**
     * Requires a notification when a data instance is created.
     *
     * @param data Unique Id of the piece of data whose existence the user wants
     * to know.
     */
    @Override
    public boolean askDataExistence(String data) {
        Node resp = getResponsibleFor(data);
        if (resp == getMe()) {
            //I'm responsible for the data control
            synchronized (dataRegistry) {
                if (dataRegistry.checkExistence(data)) {
                    return true;
                } else {
                    DataExistenceRequest der = new DataExistenceRequest(data, getMe());
                    dataRegistry.registerExistenceRequest(der);
                }
            }
        } else {
            DataExistenceRequest der = new DataExistenceRequest(data, getMe());
            CommunicationManager.notifyCommand(resp, der);
        }
        return false;
    }

    public void handleExistenceRequest(DataExistenceRequest der) {
        String data = der.getData();
        Node resp = getResponsibleFor(data);
        if (resp == getMe()) {
            //I'm responsible for the data control
            synchronized (dataRegistry) {
                if (dataRegistry.checkExistence(der.getData())) {
                    HashSet<Node> locations = dataRegistry.getLocations(data);
                    notifyDataExistence(der, locations);
                } else {
                    dataRegistry.registerExistenceRequest(der);
                }
            }
        } else if (dataManager.checkDataExistence(der.getData())) {
            HashSet<Node> locations = new HashSet<Node>();
            locations.add(getMe());
            notifyDataExistence(der, locations);
        } else {
            CommunicationManager.notifyCommand(resp, der);
        }
    }

    /**
     * Confirms the existence in the system of the requested data value to
     * another node.
     *
     * @param der information related to the data request
     */
    public void notifyDataExistence(DataExistenceRequest der, HashSet<Node> locations) {
        DataExistenceNotification den = new DataExistenceNotification(der.getData(), locations);
        CommunicationManager.notifyCommand(der.getSource(), den);
    }

    /**
     * Registers the existence of a data value.
     *
     * @param data Existing data
     */
    public void registerDataExistence(String data) {
        dataManager.registerDataExistence(data);
    }

    /**
     * Register an object value into the internal data stores and notifies to
     * the system that the value of the object is present in the local node.
     *
     * @param data identifier of the data
     * @param value object value
     * @param listener listener to notify upon changes on the request
     */
    public void storeObject(String data, Object value, DataOperationListener listener) {
        dataManager.storeObject(data, value, listener);
    }

    /**
     * Retrieves a data size.
     *
     * @param dataId identifier of the data to fetch
     * @param listener listener to notify upon changes on the request
     */
    public void obtainDataSize(String dataId, DataOperationListener listener) {
        dataManager.getSize(dataId, listener);
    }

    /**
     * Retrieves a data as an object.
     *
     * @param dataId identifier of the data to fetch
     * @param targetName identifier of the object once loaded
     * @param listener listener to notify upon changes on the request
     */
    public void obtainDataAsObject(String dataId, String targetName, DataOperationListener listener) {
        dataManager.retrieveObject(dataId, targetName, listener);
    }

    /**
     * Registers a file into the internal data stores and notifies to the system
     * that a copy of that file exists in the node.
     *
     * @param data Data Instance that identifies the file
     * @param location location where the file is stored
     * @param listener listener to notify upon changes on the request
     */
    public void storeFile(String data, String location, DataOperationListener listener) {
        dataManager.storeFile(data, location, listener);
    }

    /**
     * Retrieves a data as a file.
     *
     * @param dataId identifier of the data to fetch
     * @param targetName identifier of the file once loaded
     * @param listener listener to notify upon changes on the request
     */
    public void obtainDataAsFile(String dataId, String targetName, DataOperationListener listener) {
        dataManager.retrieveFile(dataId, targetName, listener);
    }

    @Override
    public void requestDataAsObject(String dataId) {
        Node resp = getResponsibleFor(dataId);
        if (resp == getMe()) {
            HashSet<Node> locations = dataRegistry.getLocations(dataId);
            if (locations == null || locations.isEmpty()) {
                DataSourceRequest dsr = new DataSourceRequest(dataId, getMe());
                dataRegistry.registerDataLocationsRequest(dsr);
            } else {
                Node source = pickSource(locations);
                CommunicationManager.askforDataObject(source, dataId);
            }
        } else {
            DataSourceRequest dsr = new DataSourceRequest(dataId, getMe());
            CommunicationManager.notifyCommand(resp, dsr);
        }
    }

    @Override
    public void requestDataAsFile(String dataId, String targetLocation) {
        Node resp = getResponsibleFor(dataId);
        if (resp == getMe()) {
            HashSet<Node> locations = dataRegistry.getLocations(dataId);
            if (locations == null || locations.isEmpty()) {
                DataSourceRequest dsr = new DataSourceRequest(dataId, getMe());
                dataRegistry.registerDataLocationsRequest(dsr);
                dataDestination.put(dataId, targetLocation);
            } else {
                Node source = pickSource(locations);
                CommunicationManager.askforDataFile(source, dataId, targetLocation);
            }
        } else {
            DataSourceRequest dsr = new DataSourceRequest(dataId, getMe());
            CommunicationManager.notifyCommand(resp, dsr);
            dataDestination.put(dataId, targetLocation);
        }
    }

    private Node pickSource(HashSet<Node> sources) {
        int idx = (int) (Math.random() * (double) sources.size());
        Iterator<Node> it = sources.iterator();
        Node source = it.next();
        for (int i = 0; i < idx; i++) {
            source = it.next();
        }
        return source;
    }

    public void handleSourceRequest(DataSourceRequest request) {
        String data = request.getData();
        Node resp = getResponsibleFor(data);
        if (resp == getMe()) {
            sendDataSources(data, request);
        } else {
            CommunicationManager.notifyCommand(resp, request);
        }
    }

    public void sendDataSources(String data, DataSourceRequest request) {
        HashSet<Node> locations = dataRegistry.getLocations(data);
        if (locations == null || locations.isEmpty()) {
            dataRegistry.registerDataLocationsRequest(request);
        } else {
            Node querier = request.getQuerier();
            DataSourceResponse dsp = new DataSourceResponse(data, locations);
            CommunicationManager.notifyCommand(querier, dsp);
        }

    }

    public void receivedDataSources(String dataId, HashSet<Node> locations) {
        String targetLocation = dataDestination.remove(dataId);
        if (targetLocation != null) {
            Node source = pickSource(locations);
            CommunicationManager.askforDataFile(source, dataId, targetLocation);
        } else {
            Node source = pickSource(locations);
            CommunicationManager.askforDataObject(source, dataId);
        }
    }

    /**
     * Notifies the creation of a data replica to the system.
     *
     * @param data piece of data that has been created
     */
    @Override
    public void announceDataPresence(String data) {
        Node resp = getResponsibleFor(data);
        if (resp == getMe()) {
            synchronized (dataRegistry) {
                DataRegister reg = dataRegistry.registerData(data, getMe());
                for (DataExistenceRequest request : reg.getPendingExistence()) {
                    HashSet<Node> locations = new HashSet<Node>();
                    locations.add(getMe());
                    notifyDataExistence(request, locations);
                }
                for (DataSourceRequest request : reg.getPendingLocations()) {
                    sendDataSources(data, request);
                }
            }
        } else {
            DataCreationNotification dcn = new DataCreationNotification(data, getMe());
            CommunicationManager.notifyCommand(resp, dcn);
        }

    }

    /**
     * Forwards a data creation notification received from one remote node to
     * another remote node that might have the answer.
     *
     * @param dcn notification to forward
     */
    public void handleDataCreation(DataCreationNotification dcn) {
        String data = dcn.getData();
        Node resp = getResponsibleFor(data);
        if (resp == getMe()) {
            HashSet<Node> locations = new HashSet<Node>();
            locations.add(dcn.getSource());
            synchronized (dataRegistry) {
                Node host = NodeRegistry.getRepresentive(dcn.getSource());
                DataRegister reg = dataRegistry.registerData(data, host);
                for (DataExistenceRequest request : reg.getPendingExistence()) {
                    if (!dcn.getSource().equals(request.getSource())) {
                        notifyDataExistence(request, locations);
                    }
                }
                for (DataSourceRequest request : reg.getPendingLocations()) {
                    if (!dcn.getSource().equals(request.getQuerier())) {
                        sendDataSources(data, request);
                    }
                }
            }
        } else {
            CommunicationManager.notifyCommand(resp, dcn);
        }
    }

    public void transferData(Connection connection, String dataId) {
        dataManager.transferData(dataId, connection);
    }

    @Override
    public void dataReceived(Connection c, Transfer t) {
        String receivedData = CommunicationManager.receivedData(c);
        if (t.getDestination() == Destination.FILE) {
            dataManager.receivedFile(receivedData, t.getFileName());
        } else {
            dataManager.receivedObject(receivedData, t.getArray());
        }
    }

    public abstract void updateMaster(Node node);

    @Override
    public void errorHandler(Connection c, Transfer t, CommException e) {
        LOGGER.error("Error on connection " + c + ".", e);
    }

    @Override
    public void writeFinished(Connection c, Transfer t) {

    }

    @Override
    public void connectionFinished(Connection c) {

    }

    @Override
    public void shutdown() {

    }

    /*By default, do nothing. If the node can handle a new job execution
         request, the node will override the method.*/
    public abstract void newJob(Job job);

    /*By default, do nothing. If the node can handle a job execution
         notification, the node will override the method.*/
    public abstract void jobFinished(int taskId, int platformId, JobProfile job, Node executor);

}
