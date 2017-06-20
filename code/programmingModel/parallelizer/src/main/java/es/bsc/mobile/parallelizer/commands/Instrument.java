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
package es.bsc.mobile.parallelizer.commands;

import es.bsc.mobile.parallelizer.configuration.Paths;
import es.bsc.mobile.parallelizer.utils.InstrumentationAssistant;
import es.bsc.mobile.parallelizer.utils.MethodEditor;
import java.io.IOException;
import java.util.TreeSet;
import java.util.logging.Logger;
import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.NotFoundException;

/**
 * The Instrument class encapsulates the execution of the instrument command
 * which instruments the application classes to replace the CE invocations by
 * calls to the COMPSs runtime toolkit.
 *
 * For this purpose, when executed, the command instruments the code of all the
 * application classes and modifies the onCreate/onDestroy methods of the main
 * components of the application to instantiate the runtime service stub.
 *
 * @author flordan
 */
public class Instrument implements Command {

    private static final Logger LOGGER = Logger.getLogger("INSTRUMENTER");

    private static final String CEI_NOT_FOUND = "Could find Core Element Interface (CEI.java).";
    private static final String LOAD_UTILS_ERROR = "Error loading class for editing.";
    private static final String ERROR_INSTRUMENTING = "Error instrumenting class ";

    private static final ClassPool cp = ClassPool.getDefault();

    @Override
    public void execute(String projectDir, Paths paths) throws CommandExecutionException {

        String classesDir = projectDir + paths.compiledClassesDir();
        try {
            InstrumentationAssistant.load(classesDir);
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
            throw new CommandExecutionException(LOAD_UTILS_ERROR, ex);
        } catch (NotFoundException ex) {
            ex.printStackTrace();
            throw new CommandExecutionException(LOAD_UTILS_ERROR, ex);
        }

        final Class<?> cei;
        try {
            cei = Class.forName("CEI");
        } catch (ClassNotFoundException ex) {
            throw new CommandExecutionException(CEI_NOT_FOUND, ex);
        }
        /*        try {
         copyRuntime(projectDir);
         } catch (IOException ex) {
         throw new CommandExecutionException(COPY_RUNTIME_ERROR, ex);
         }
         */
        TreeSet<String> classes = new TreeSet<String>();
        classes.addAll(InstrumentationAssistant.getOrchestrationClasses());
        classes.addAll(InstrumentationAssistant.getMainClasses());
        for (String className : classes) {
            try {
                modifyClass(classesDir, className, cei);
            } catch (NotFoundException ex) {
                throw new CommandExecutionException(ERROR_INSTRUMENTING + className, ex);
            } catch (CannotCompileException ex) {
                throw new CommandExecutionException(ERROR_INSTRUMENTING + className, ex);
            } catch (IOException ex) {
                throw new CommandExecutionException(ERROR_INSTRUMENTING + className, ex);
            }
        }
    }

    private void modifyClass(String classesDir, String className, Class<?> cei) throws NotFoundException, CannotCompileException, IOException {
        CtClass appClass = cp.get(className);
        boolean isOrchestration = InstrumentationAssistant.isOrchestratonClass(className);
        boolean isMain = InstrumentationAssistant.isMainClass(className);

        // Create Code Converter
        CodeConverter converter = new CodeConverter();
        CtClass arrayWatcher = cp.get("es.bsc.mobile.runtime.ArrayAccessWatcher");
        CodeConverter.DefaultArrayAccessReplacementMethodNames names = new CodeConverter.DefaultArrayAccessReplacementMethodNames();
        converter.replaceArrayAccess(arrayWatcher, (CodeConverter.ArrayAccessReplacementMethodNames) names);

        MethodEditor methodeditor = new MethodEditor(cei.getDeclaredMethods());
        for (CtMethod m : appClass.getDeclaredMethods()) {
            LOGGER.info("Instrumenting method: " + m.getLongName());
            if (isOrchestration) {
                m.instrument(converter);
                m.instrument(methodeditor);
            }
            if (isMain && m.getName().equals("main") && m.getSignature().equals("([Ljava/lang/String;)V")) {
                LOGGER.info("Adding es.bsc.mobile.runtime.Runtime.startRuntime();");
                m.insertBefore("es.bsc.mobile.runtime.Runtime.startRuntime();");
            }
        }

        // Salvar la classe
        appClass.writeFile(classesDir);
    }
}
