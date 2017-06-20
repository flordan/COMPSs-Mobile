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
package es.bsc.comm.nio.event;

import es.bsc.comm.EventManager;
import es.bsc.comm.nio.NIOConnection;
import es.bsc.comm.stage.Stage;

public class NewTransferEvent extends NIOEvent {

    private final Stage transfer;

    public NewTransferEvent(NIOConnection c, Stage t) {
        super(c);
        this.transfer = t;
    }

    @Override
    public EventType getEventType() {
        return EventType.NEW_TRANSFER;
    }

    @Override
    public void processEventOnConnection(EventManager<?> ntm) {
        getConnection().requestStage(transfer);
    }

    @Override
    public String toString() {
        return "New Transfer Event for connection " + getConnection() + " with transfer " + transfer;
    }
}
