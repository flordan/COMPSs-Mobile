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
package es.bsc.mobile.runtime;

import es.bsc.comm.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.MessageHandler;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;
import es.bsc.mobile.comm.CommunicationManager;
import es.bsc.mobile.runtime.service.RuntimeServiceItf;
import es.bsc.mobile.runtime.types.CEI;
import es.bsc.mobile.runtime.types.Task;
import es.bsc.mobile.runtime.types.data.DataInfo;
import es.bsc.mobile.runtime.types.data.DataInstance;
import es.bsc.mobile.runtime.types.data.RegisteredObject;
import es.bsc.mobile.runtime.types.data.access.DataAccess;
import es.bsc.mobile.runtime.types.data.access.ReadAccess;
import es.bsc.mobile.runtime.types.data.access.ReadWriteAccess;
import es.bsc.mobile.runtime.types.data.parameter.BasicTypeParameter;
import es.bsc.mobile.runtime.types.data.parameter.FileParameter;
import es.bsc.mobile.runtime.types.data.parameter.Parameter;
import es.bsc.mobile.runtime.types.data.parameter.RegisteredParameter;
import es.bsc.mobile.runtime.utils.CoreManager;
import es.bsc.mobile.runtime.utils.DataRegistry;
import es.bsc.mobile.types.comm.Node;
import es.bsc.mobile.types.messages.runtime.DataExistenceNotification;
import es.bsc.mobile.types.messages.runtime.DataSourceResponse;
import es.bsc.mobile.types.messages.runtime.DataTransferRequest;
import es.bsc.mobile.utils.Serializer;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Runtime implements MessageHandler {

    public static final int ARGUMENTS_PER_PARAMETER = 3;
    public static final int ARGUMENT_TYPE_OFFSET = 0;
    public static final int ARGUMENT_DIRECTION_OFFSET = 1;
    public static final int ARGUMENT_VALUE_OFFSET = 2;

    private static final Logger LOGGER = LogManager.getLogger("Runtime");
    private static final Node ME = new Node(null, 28000);
    private static final RuntimeServiceItf RUNTIME_SERVICE = RuntimeServiceItf.Stub.asInterface();

    private static final DataRegistry<Integer> PRIVATE_REG = new DataRegistry<>();
    private static final HashMap<Integer, Object> ID_TO_REPRESENTATIVE = new HashMap<>();
    private static final HashMap<String, LinkedList<Operation>> PENDING_OPERATIONS = new HashMap<>();

    private static CEI cei;

    private Runtime() {
    }

    /**
     * *********************************************************************
     * *********************************************************************
     * **************** ---------------------------------- *****************
     * **************** | INVOCATIONS FROM THE MAIN CODE | *****************
     * **************** ---------------------------------- *****************
     * *********************************************************************
     * *********************************************************************
     */
    /**
     * Initializes all the runtime structures and launches the threads.
     *
     */
    public static void startRuntime() {

        CommunicationManager.start(new Runtime());

        try {
            if (cei == null) {
                cei = new CEI(Class.forName("CEI"));
                if (!CoreManager.isInitialized()) {
                    RUNTIME_SERVICE.start(cei);
                    String[] signatures = cei.getAllSignatures();
                    int[] ids = RUNTIME_SERVICE.getCoreIds(signatures);
                    for (int i = 0; i < ids.length; i++) {
                        CoreManager.registerSignature(signatures[i], ids[i]);
                    }
                }
            }
        } catch (ClassNotFoundException e) {
            LOGGER.error("Core Element Interface not found. " + e.getMessage());
        }
    }

    public static void unbindRuntime() {
        //No need to do anything for the Java runtime
    }

    /**
     * Registers an object access from the main code of the application.
     *
     * Should be invoked when any object is accessed.
     *
     * @param <T> Type of the object to be registered
     * @param o Accessed representative object
     * @param isWritter {true} if the access modifies the content of the object
     * @return The current object value
     */
    public static <T> T newObjectAccess(T o, boolean isWritter) {
        if (o == null) {
            return null;
        }
        int dataId = getDataId(o);
        DataInfo data = PRIVATE_REG.findData(dataId);
        if (data == null) {
            // Data is not in the system return same value
            return o;
        }
        DataAccess da = null;
        try {
            da = PRIVATE_REG.registerLocalDataAccess(isWritter ? Direction.INOUT : Direction.IN, data);
        } catch (IOException e) {
            LOGGER.fatal("Error obtaining object");
            System.exit(1);
        }
        RegisteredObject<T> rd = (RegisteredObject<T>) data.getCurrentVersion().getValue();
        synchronized (rd) {
            if (!rd.isLocal()) {
                // We bring it
                String inRenaming;
                String outRenaming;
                if (isWritter) {
                    DataInstance inInstance = ((ReadWriteAccess) da).getReadDataInstance();
                    DataInstance outInstance = ((ReadWriteAccess) da).getWrittenDataInstance();
                    inRenaming = inInstance.getRenaming();
                    outRenaming = outInstance.getRenaming();
                    if (inInstance != outInstance) {
                        RUNTIME_SERVICE.notifyDataCreation(outRenaming, ME);
                    }

                } else {
                    inRenaming = ((ReadAccess) da).getReadDataInstance().getRenaming();
                    outRenaming = inRenaming;
                }
                Operation op = new Operation(dataId, inRenaming, outRenaming);
                LinkedList<Operation> ops = PENDING_OPERATIONS.get(inRenaming);
                if (ops == null) {
                    ops = new LinkedList<>();
                    PENDING_OPERATIONS.put(inRenaming, ops);
                }
                ops.add(op);
                // We ask for it
                RUNTIME_SERVICE.requestDataExistence(inRenaming, ME);
            }
        }

        try {
            return rd.getValue();
        } catch (Exception e) {
            LOGGER.fatal("Error obtaining object");
            System.exit(1);
        }
        return null;
    }

    /**
     * Registers an object access from the main code, obtains its value and
     * removes any data stored in the runtime related to it.
     *
     * This method should be called when a task returns a primitive value and
     * this value is recovered by the runtime.
     *
     * @param <T> Type of the object value
     * @param o Accessed representative object
     * @return final value of the primitive value
     * @throws InterruptedException Interruption while the value is computed or
     * brough back to the master.
     */
    public static <T> T getValueAndRemove(T o)
            throws InterruptedException {
        int dataId = getDataId(o);
        DataInfo data = PRIVATE_REG.findData(dataId);
        if (data == null) {
            // Data is not in the system return same value
            return null;
        }
        RegisteredObject<T> rd = (RegisteredObject<T>) PRIVATE_REG.getCurrentVersionValue(data);
        synchronized (rd) {
            if (!rd.isLocal()) {
                String inRenaming = data.getCurrentVersion().getDataInstance().getRenaming();
                Operation op = new Operation(dataId, inRenaming, inRenaming);
                LinkedList<Operation> ops = PENDING_OPERATIONS.get(inRenaming);
                if (ops == null) {
                    ops = new LinkedList<>();
                    PENDING_OPERATIONS.put(inRenaming, ops);
                }
                ops.add(op);

                RUNTIME_SERVICE.requestDataExistence(inRenaming, ME);

            }
        }
        return rd.getValue();
    }

    /**
     * Generates a new task whose execution will be managed by the runtime.
     *
     * @param methodClass name of the class containing the method that has been
     * invoked
     * @param methodName name of the invoked method
     * @param hasTarget the method has been invoked on a callee object
     * @param parameters parameter values
     * @return a unique identifier for the task
     */
    public static int executeTask(String methodClass, String methodName,
            boolean hasTarget, Object... parameters) {
        Parameter[] params = null;
        try {
            params = processParameters(parameters);
        } catch (URISyntaxException | IOException ex) {
            LOGGER.error("Error preparing object parameters for remote execution.", ex);
            System.exit(1);
        }
        Task t = new Task(methodClass, methodName, hasTarget, params);
        RUNTIME_SERVICE.executeTask(t);
        return t.getId();
    }

    private static Parameter[] processParameters(Object[] parameters)
            throws URISyntaxException, IOException {
        int parameterCount = parameters.length / ARGUMENTS_PER_PARAMETER;
        Parameter[] params = new Parameter[parameterCount];
        for (int paramIdx = 0; paramIdx < parameterCount; paramIdx++) {
            Type type = (Type) parameters[paramIdx * ARGUMENTS_PER_PARAMETER + ARGUMENT_TYPE_OFFSET];
            Direction direction = (Direction) parameters[paramIdx * ARGUMENTS_PER_PARAMETER + ARGUMENT_DIRECTION_OFFSET];
            switch (type) {
                case OBJECT:
                    Object object = parameters[paramIdx * ARGUMENTS_PER_PARAMETER + ARGUMENT_VALUE_OFFSET];
                    int dataId = getDataId(object);
                    DataInfo dataInfo = PRIVATE_REG.findData(dataId);
                    if (dataInfo == null) {
                        ID_TO_REPRESENTATIVE.put(dataId, object);
                        RegisteredObject paramObject = new RegisteredObject(object);
                        dataInfo = PRIVATE_REG.registerData(dataId, paramObject);

                        if (direction != Direction.OUT) {
                            RUNTIME_SERVICE.notifyDataCreation(dataInfo.getCurrentVersion().getDataInstance().getRenaming(), ME);
                        }
                    }
                    DataAccess da = PRIVATE_REG.registerRemoteDataAccess(direction, dataInfo);
                    params[paramIdx] = new RegisteredParameter(Type.OBJECT, direction, da);
                    break;
                case FILE:
                    params[paramIdx] = new FileParameter(direction, (String) parameters[paramIdx * ARGUMENTS_PER_PARAMETER + ARGUMENT_VALUE_OFFSET]);
                    break;
                default:
                    // Basic Type
                    params[paramIdx] = new BasicTypeParameter(type, Direction.IN, parameters[paramIdx * ARGUMENTS_PER_PARAMETER + ARGUMENT_VALUE_OFFSET]);
            }
        }
        return params;
    }

    /**
     * Indicates that an app does not generate any more tasks thus, all the
     * internal objects can be deleted once all the tasks are done.
     *
     * @param appId Id of the application that ends.
     */
    public void noMoreTasks(long appId) {
        //Not implemented yet
    }

    /**
     * Indicates that a file has to be deleted from the whole system
     *
     * @param fileName name of the file to be rmeoved
     * @return
     */
    public boolean deleteFile(String fileName) {
        //Not implemented yet
        return true;
    }

    /**
     * *********************************************************************
     * *********************************************************************
     * ************* ---------------------------------------- **************
     * ************* | INVOCATIONS FROM OTHER RUNTIME PARTS | **************
     * ************* ---------------------------------------- **************
     * *********************************************************************
     * *********************************************************************
     */
    @Override
    public void init() {
        try {
            CommunicationManager.openServer(ME);
        } catch (CommException e) {
            LOGGER.fatal("Error starting server socket on Application side.", e);
        }
    }

    @Override
    public void commandReceived(Connection c, Transfer t) {
        try {
            if (t.getObject() instanceof DataTransferRequest) {
                DataTransferRequest dtr = (DataTransferRequest) t.getObject();
                String rData = dtr.getDataSource();
                RegisteredObject rd = (RegisteredObject) PRIVATE_REG.getRegisteredData(rData);
                if (!rd.isSavedForTransfer()) {
                    rd.saveForTransfer();
                }
                byte[] array = rd.getSerialized();
                c.sendDataArray(array);
                c.finishConnection();
            }
            if (t.getObject() instanceof DataExistenceNotification) {
                c.finishConnection();
                DataExistenceNotification den = (DataExistenceNotification) t.getObject();
                String data = den.getData();
                if (den.getLocations().isEmpty()) {
                    RUNTIME_SERVICE.requestDataLocations(data, ME);
                } else {
                    es.bsc.comm.Node source = den.getLocations().iterator().next();
                    completeSource((NIONode) source, c);
                    CommunicationManager.askforDataObject(source, data);
                }
            }
            if (t.getObject() instanceof DataSourceResponse) {
                c.finishConnection();
                DataSourceResponse dsr = (DataSourceResponse) t.getObject();
                String data = dsr.getData();
                es.bsc.comm.Node source = dsr.getLocations().iterator().next();
                completeSource((NIONode) source, c);
                CommunicationManager.askforDataObject(source, data);
            }
        } catch (IOException e) {
            LOGGER.error("Error processing received command.", e);
        }
    }

    @Override
    public void errorHandler(Connection cnctn, Transfer trnsfr, CommException ce) {
        LOGGER.error("Error processing transfer " + trnsfr + " on connection " + cnctn, ce);
    }

    @Override
    public void dataReceived(Connection cnctn, Transfer trnsfr) {
        String daId = CommunicationManager.receivedData(cnctn);
        LinkedList<Operation> ops = PENDING_OPERATIONS.get(daId);
        for (Operation op : ops) {
            RegisteredObject rd = (RegisteredObject) PRIVATE_REG.getRegisteredData(op.target);
            Object o = null;
            if (trnsfr.isObject()) {
                o = trnsfr.getObject();
            }
            if (trnsfr.isArray()) {
                byte[] array = trnsfr.getArray();
                if (op.source.equals(op.target)) {
                    rd.setSerialized(array);
                }
                try {
                    o = Serializer.deserialize(array);
                } catch (IOException | ClassNotFoundException e) {
                    LOGGER.error("Could not deserialize obtained object", e);
                }
            }
            rd.setValue(o);
        }
    }

    @Override
    public void writeFinished(Connection cnctn, Transfer trnsfr) {
        //End of transfer submission do nothing
    }

    @Override
    public void connectionFinished(Connection cnctn) {
        //Closed connection
    }

    @Override
    public void shutdown() {
        //Nothing to do on shutdown
    }

    private void completeSource(NIONode node, Connection connection) {
        if (node.getIp() == null) {
            String ip = ((NIONode) ((NIOConnection) connection).getNode()).getIp();
            node.setIp(ip);
        }
    }

    private static class Operation {

        private final Integer data;
        private final String source;
        private final String target;

        public Operation(int data, String source, String target) {
            this.data = data;
            this.source = source;
            this.target = target;
        }

        public int getData() {
            return data;
        }

        public String getTarget() {
            return target;
        }

        @Override
        public String toString() {
            return "object " + data + " with renaming " + target;
        }

    }

    private static Integer getDataId(Object o) {
        int hashCode = o.hashCode();
        while (true) {
            Object rep = ID_TO_REPRESENTATIVE.get(hashCode);
            if (rep == null || rep == o) {
                return hashCode;
            }
            hashCode++;
        }
    }
}
