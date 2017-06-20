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
package es.bsc;

import es.bsc.mobile.parallelizer.commands.Command;
import es.bsc.mobile.parallelizer.commands.Instrument;
import es.bsc.mobile.parallelizer.configuration.Paths;
import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Instruments the classes of the application to include the necessary
 * invocations to the COMPSs-Mobile runtime.
 *
 * @author Francesc Lordan
 * @version 1.0
 * @since 1.0
 * @goal parallelize
 * @phase compile
 * @threadSafe
 * @requiresDependencyResolution compile
 */
public class COMPSsMobile extends AbstractMojo {

    /**
     * Project classpath.
     *
     * @parameter default-value="${project.compileClasspathElements}"
     * @required
     * @readonly
     */
    private List<String> classpathElements;

    /**
     * <p>
     * Specify where to place generated source files created by annotation
     * processing. Only applies to JDK 1.6+
     * </p>
     *
     * @parameter default-value="${project.build.directory}"
     * @since 2.2
     */
    private File buildDirectory;

    @Override
    public void execute() throws MojoExecutionException {
        Command command;
        getLog().info("Instrumenting classes on directory " + buildDirectory.getAbsolutePath() + " to be parallelized.");
        command = new Instrument();
        try {
            LinkedList<String> classpath = new LinkedList<String>();
            classpath.addAll(classpathElements);
            File f = new File(buildDirectory, "classes");
            classpath.add(f.getAbsolutePath());

            for (String dependency : classpath) {
                f = new File(dependency);
                URL u = f.toURI().toURL();
                URLClassLoader urlClassLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
                Class urlClass = URLClassLoader.class;
                Method method = urlClass.getDeclaredMethod("addURL", new Class[]{URL.class});
                method.setAccessible(true);
                method.invoke(urlClassLoader, new Object[]{u});
            }

            Logger.getLogger("INSTRUMENTER").setLevel(Level.ALL);
            Logger.getLogger("INSTRUMENTER").setUseParentHandlers(false);
            Logger.getLogger("INSTRUMENTER").addHandler(new MavenLogHandler());

            command.execute(buildDirectory.getAbsolutePath(), new Paths.Maven());

        } catch (Exception e) {
            throw new MojoExecutionException("Error instrumenting application classes.", e);
        }
    }

    private class MavenLogHandler extends Handler {

        @Override
        public void publish(LogRecord record) {
            if (record.getLevel().intValue() > 899) {
                if (record.getLevel().intValue() > 999) {
                    getLog().error(record.getMessage());
                } else {
                    getLog().warn(record.getMessage());
                }
            } else if (record.getLevel().intValue() > 599) {
                getLog().info(record.getMessage());
            } else {
                getLog().debug(record.getMessage());
            }
        }

        @Override
        public void flush() {

        }

        @Override
        public void close() throws SecurityException {

        }
    }
}
