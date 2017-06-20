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
package es.bsc.mobile.utils;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class LoggerConfiguration {

    private static final Configuration CONF;
    private static final ConsoleAppender CONSOLE_APPENDER;

    static {
        CONF = ((LoggerContext) LogManager.getContext(false)).getConfiguration();

        PatternLayout layout = PatternLayout.newBuilder().withPattern("[%c] %d{mm:ss,SSS}- %m%n").build();
        ConsoleAppender.Builder consoleBuilder = new ConsoleAppender.Builder();
        consoleBuilder.setName("ConsoleAppender");
        consoleBuilder.setLayout(layout);
        CONSOLE_APPENDER = consoleBuilder.build();
        CONSOLE_APPENDER.start();
        CONF.addAppender(CONSOLE_APPENDER);
    }

    public static void configRootLogger(Level level) {
        CONF.getRootLogger().setLevel(Level.OFF);
        CONF.getRootLogger().addAppender(CONSOLE_APPENDER, Level.INFO, null);
    }

    public static void configLogger(String name, Level level) {
        LoggerConfig lc = new LoggerConfig(name, level, false);
        lc.addAppender(CONSOLE_APPENDER, level, null);
        CONF.addLogger(name, lc);
    }
}
