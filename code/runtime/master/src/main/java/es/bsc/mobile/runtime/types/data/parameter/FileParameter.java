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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import es.bsc.mobile.annotations.Parameter.Direction;
import es.bsc.mobile.annotations.Parameter.Type;
import java.io.Serializable;

public class FileParameter extends Parameter implements Serializable {

    // URI beginning
    protected static final String FILE_URI = "file:";
    protected static final String SHARED_URI = "shared:";

    private final String host;
    private final String path;
    private final String fileName;

    public FileParameter(Direction direction, String fullName)
            throws URISyntaxException, IOException {
        super(Type.FILE, direction);
        String[] file = extractHostPathName(fullName);
        host = file[0];
        path = file[1];
        fileName = file[2];
    }

    @Override
    public String toString() {
        return host + ":" + path + fileName + " " + getType() + " " + getDirection();
    }

    public String getFullFileName() {
        return path + fileName;
    }

    protected final String[] extractHostPathName(String fullName)
            throws URISyntaxException, IOException {
        String name;
        String extractedPath;
        String extractedHost;

        if (fullName.startsWith(FILE_URI)) {
            /*
             * URI syntax with host name and absolute path, e.g.
             * "file://bscgrid01.bsc.es/home/etejedor/file.txt" Only used in
             * grid-aware applications, using IT API and partial loader, since
             * total loader targets sequential applications that use local
             * files.
             */
            URI u = new URI(fullName);
            extractedHost = u.getHost();
            String fullPath = u.getPath();
            int pathEnd = fullPath.lastIndexOf("/");
            extractedPath = fullPath.substring(0, pathEnd + 1);
            name = fullPath.substring(pathEnd + 1);
        } else if (fullName.startsWith(SHARED_URI)) {
            URI u = new URI(fullName);
            extractedHost = "shared:" + u.getHost();
            String fullPath = u.getPath();
            int pathEnd = fullPath.lastIndexOf("/");
            extractedPath = fullPath.substring(0, pathEnd + 1);
            name = fullPath.substring(pathEnd + 1);
        } else {
            // Local file, format will depend on OS
            File f = new File(fullName);
            String canonicalPath = f.getCanonicalPath();
            name = f.getName();
            extractedPath = canonicalPath.substring(0,
                    canonicalPath.length() - name.length());
            extractedHost = "master";
        }
        return new String[]{extractedHost, extractedPath, name};
    }
}
