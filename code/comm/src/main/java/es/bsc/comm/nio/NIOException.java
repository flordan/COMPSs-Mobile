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
package es.bsc.comm.nio;

import es.bsc.comm.CommException;

public class NIOException extends CommException {

    private static final long serialVersionUID = 1L;

    public enum ErrorType {

        LOADING_EVENT_MANAGER_CLASS,
        LOADING_LISTENER,
        EVENT_MANAGER_INIT,
        MESSAGE_HANDLER_INIT,
        STARTING_SERVER,
        STARTING_CONNECTION,
        RESTARTING_CONNECTION,
        ACCEPTING_CONNECTION,
        FINISHING_CONNECTION,
        READ,
        WRITE,
        CLOSED_CONNECTION
    }

    private final ErrorType error;

    public NIOException(ErrorType error, Throwable t) {
        super(t);
        this.error = error;
    }

    public NIOException(ErrorType error, Exception e) {
        super(e);
        this.error = error;
    }

    public ErrorType getError() {
        return error;
    }

}
