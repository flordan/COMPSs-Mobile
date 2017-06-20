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
package es.bsc.mobile.comm;

import es.bsc.comm.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.MessageHandler;
import es.bsc.comm.Node;
import es.bsc.comm.TransferManager;
import es.bsc.mobile.types.messages.runtime.DataTransferRequest;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The CommunicationManager class is a utility class to manage all the
 * communications among two different nodes of the runtime: commands submissions
 * and data transfers (requests and data transmission).
 *
 * @author flordan
 */
public class CommunicationManager {

    protected static final String EM_CLASS = "es.bsc.comm.nio.NIOEventManager";

    private static final String NOTIFYING = "Notifying ";
    private static final String ASKING = "Asking for ";
    private static final String SENDING = "Sending ";
    private static final String RECEIVED = "Received ";

    private static final String COMMAND = "command ";
    private static final String DATA = "data ";
    private static final String OBJECT = "object ";
    private static final String FILE = "file ";
    private static final String ARRAY = "array ";

    private static final String TO = " to ";
    private static final String VIA = " via ";
    private static final String END = ".";

    private static final Logger LOGGER = LogManager.getLogger("Runtime.Communications");
    private static final boolean LOG_ENABLED = LOGGER.isInfoEnabled();

    private static final HashMap<Connection, String> REQUESTED_DATA = new HashMap<>();
    private static final TransferManager TM = new TransferManager();

    private CommunicationManager() {
    }

    public static void start(MessageHandler mh) {
        try {
            TM.init(EM_CLASS, null, mh);
            TM.start();
        } catch (CommException ce) {
            LOGGER.fatal("Could not start the Communication worker", ce);
        }
    }

    public static void openServer(Node serverNode) throws CommException {
        TM.startServer(serverNode);
    }

    /**
     * Sends a command to the target node.
     *
     * It opens a client connection to the remote node, serializes and ships the
     * command passes as a parameter through the connection and closes the
     * connection on the client side.
     *
     * @param target Receiver of the command
     * @param command Command to submit
     */
    public static void notifyCommand(Node target, Object command) {
        Connection c = TM.startConnection(target);
        if (LOG_ENABLED) {
            LOGGER.info(NOTIFYING + COMMAND + command + TO + target + VIA + c + END);
        }
        c.sendCommand(command);
        c.finishConnection();
    }

    /**
     * Asks a remote node to transfer some data to store it as an object.
     *
     * The method opens a client connection with the remote node to host the
     * data transfer. Once the connection is open, it registers which data
     * should be received through the open connection and submits a data request
     * to the node. After that, the connection waits to receive the data object.
     *
     * @param source Node who already holds the data
     * @param data Identifier of the data that should be received.
     */
    public static void askforDataObject(Node source, String data) {
        DataTransferRequest dtr = new DataTransferRequest(data);
        Connection c = TM.startConnection(source);
        if (LOG_ENABLED) {
            LOGGER.info(ASKING + OBJECT + data + TO + source + VIA + c + END);
        }
        REQUESTED_DATA.put(c, data);
        c.sendCommand(dtr);
        c.receiveDataArray();
    }

    /**
     * Asks a remote node to transfer some data to store it as a file.
     *
     * The method opens a client connection with the remote node to host the
     * data transfer. Once the connection is open, it registers which data
     * should be received through the open connection and submits a data request
     * to the node. After that, the connection waits to receive the data and
     * write it down to the local storage device.
     *
     * @param source Node who already holds the data
     * @param data Identifier of the data that should be received.
     * @param file Path of the file where to write the data down
     */
    public static void askforDataFile(Node source, String data, String file) {
        DataTransferRequest dtr = new DataTransferRequest(data);
        Connection c = TM.startConnection(source);
        if (LOG_ENABLED) {
            LOGGER.info(ASKING + FILE + data + TO + source + VIA + c + END);
        }
        REQUESTED_DATA.put(c, data);
        c.sendCommand(dtr);
        c.receiveDataFile(file);
    }

    /**
     * Sends a byte array through an already open connection.
     *
     * It ships the array through the connection and then, it the connection is
     * closed. Generally, this method is called after receiving a Data Transfer
     * Request through the same connection.
     *
     * @param c Open connection
     * @param array Array to transfer
     */
    public static void transferDataArray(Connection c, byte[] array) {
        if (LOG_ENABLED) {
            LOGGER.info(SENDING + array.length + " bytes length" + ARRAY + VIA + c + END);
        }
        c.sendDataArray(array);
        c.finishConnection();
    }

    /**
     *
     * Sends an object through an already open connection.
     *
     * It ships an object, after serializing it into byte array, through the
     * connection. Then, the connection is closed. Generally, this method is
     * called after receiving a Data Transfer Request through the same
     * connection.
     *
     * @param c Open connection
     * @param o Object to transfer
     */
    public static void transferDataObject(Connection c, Object o) {
        if (LOG_ENABLED) {
            LOGGER.info(SENDING + OBJECT + Integer.toString(o.hashCode()) + VIA + c + END);
        }
        c.sendDataObject(o);
        c.finishConnection();
    }

    /**
     *
     * Sends a file content through an already open connection.
     *
     * It read the content of the file and submits it through through the
     * connection and the closes the connection. Generally, this method is
     * called after receiving a Data Transfer Request through the same
     * connection.
     *
     * @param c Open connection
     * @param file Path of the file to send
     */
    public static void transferDataFile(Connection c, String file) {
        if (LOG_ENABLED) {
            LOGGER.info(SENDING + FILE + file + VIA + c + END);
        }
        c.sendDataFile(file);
        c.finishConnection();
    }

    /**
     * Registers that some previously requested data has been received through a
     * connection and identifies which data has been received.
     *
     * @param c Connection transferring the data
     * @return Identifier of the received data
     */
    public static String receivedData(Connection c) {
        String dataRef = REQUESTED_DATA.remove(c);
        if (LOG_ENABLED) {
            LOGGER.info(RECEIVED + DATA + dataRef + VIA + c + END);
        }
        c.finishConnection();
        return dataRef;

    }

    /**
     * Closes a connection after it has received some data that won't be
     * replied.
     *
     * @param c Connection to close.
     */
    public static void receivedNotification(Connection c) {
        c.finishConnection();
    }
}
