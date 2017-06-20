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
package es.bsc.comm;

/**
 * Connection is the abstract base class to represent all the connections
 * between the local node and a remote node.
 *
 * The connection class encapsulates all the state information required to
 * trasmit info to and receive values from a the remote node. Currently, through
 * a connection, an trasmitter application can order commands and submit data
 * stored in files, objects or byte arrays. From the receiver side, it can
 * specify how to store the received data whether if it is a file or a memory
 * structure as objects or a byte array.
 *
 * @author flordan
 */
public interface Connection {

    /**
     * Sends a command through the connection
     *
     * @param cmd Command to submit through the connection
     */
    public void sendCommand(Object cmd);

    /**
     * Sends data stored in a file through the connection.
     *
     * @param name Location of the file that will be submitted
     */
    public void sendDataFile(String name);

    /**
     * Sends an object through the connection.
     *
     * @param o Object that will be submitted
     */
    public void sendDataObject(Object o);

    /**
     * Sends the data stored in a byte arrey through the connection.
     *
     * @param array Data to be transferred through the connection.
     */
    public void sendDataArray(byte[] array);

    /**
     * Enables the connection to receive some command or data and notify it
     */
    public void receive();

    /**
     * Enable the connection to receive a command or some data. In case of
     * receiving a data that originally was stored in a file, the data will be
     * saved in a file.
     *
     * @param name Location of the file where data can potentially be saved
     */
    public void receive(String name);

    /**
     * Enables the connection to receive some data and store it as an object
     */
    public void receiveDataObject();

    /**
     * Enables the connection to receive some data and store it as a byte array
     */
    public void receiveDataArray();

    /**
     * Enables the connection to receive some data and store it in a file
     *
     * @param name Location of the file where to save the received data
     */
    public void receiveDataFile(String name);

    /**
     * Closes the connection after all the previous ordered transfers (Receives
     * and Transmit) have been processed.
     */
    public void finishConnection();

    /**
     * Return the remote node involved in the connection
     *
     * @return the remote node involved in the connection
     */
    public Node getNode();

}
