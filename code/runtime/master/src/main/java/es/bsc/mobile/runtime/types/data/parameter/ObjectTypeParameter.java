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
package es.bsc.mobile.runtime.types.data.parameter;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;

public class ObjectTypeParameter extends Parameter implements Serializable {

    protected Object value;

    public ObjectTypeParameter(es.bsc.mobile.annotations.Parameter.Direction direction, Object value)
            throws URISyntaxException, IOException {
        super(es.bsc.mobile.annotations.Parameter.Type.OBJECT, direction);
        this.value = value;
    }

}
