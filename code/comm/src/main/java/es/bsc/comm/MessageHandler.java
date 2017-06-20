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

import es.bsc.comm.stage.Transfer;

public interface MessageHandler {

    // Initialization
    public void init() throws CommException;

    // The transfer could not be completed
    public void errorHandler(Connection c, Transfer t, CommException e);

    // New data received
    public void dataReceived(Connection c, Transfer t);

    // New command received
    public void commandReceived(Connection c, Transfer t);

    // A send transfer has finished
    public void writeFinished(Connection c, Transfer t);

    // A connection has finished
    public void connectionFinished(Connection c);

    // Shutdown method
    public void shutdown();
}
