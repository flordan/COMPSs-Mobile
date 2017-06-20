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
package es.bsc.mobile.parallelizer.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.logging.Level;
import java.util.logging.Logger;

import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import javassist.NotFoundException;
import javassist.expr.ExprEditor;
import javassist.expr.FieldAccess;
import javassist.expr.MethodCall;
import javassist.expr.NewExpr;

public class MethodEditor extends ExprEditor {

    private static final Logger LOGGER = Logger.getLogger("INSTRUMENTER");

    private final Method[] ceiMethods;
    private static final String RUNTIME = "es.bsc.mobile.runtime.Runtime";
    private static final String ARRAY_ACCESS_WATCHER = "es.bsc.mobile.runtime.ArrayAccessWatcher";

    private static final String OBJECT = "es.bsc.mobile.annotations.Parameter.Type.OBJECT";
    private static final String FILE = "es.bsc.mobile.annotations.Parameter.Type.FILE";
    private static final String STRING = "es.bsc.mobile.annotations.Parameter.Type.STRING";
    private static final String BOOLEAN = "es.bsc.mobile.annotations.Parameter.Type.BOOLEAN";
    private static final String CHAR = "es.bsc.mobile.annotations.Parameter.Type.CHAR";
    private static final String BYTE = "es.bsc.mobile.annotations.Parameter.Type.BYTE";
    private static final String SHORT = "es.bsc.mobile.annotations.Parameter.Type.SHORT";
    private static final String INT = "es.bsc.mobile.annotations.Parameter.Type.INT";
    private static final String LONG = "es.bsc.mobile.annotations.Parameter.Type.LONG";
    private static final String FLOAT = "es.bsc.mobile.annotations.Parameter.Type.FLOAT";
    private static final String DOUBLE = "es.bsc.mobile.annotations.Parameter.Type.DOUBLE";
    private static final String DEFAULT_BOOLEAN = "new Boolean(false)";
    private static final String DEFAULT_CHAR = "new Character(Character.MIN_VALUE)";
    private static final String DEFAULT_BYTE = "new Byte(Byte.MIN_VALUE)";
    private static final String DEFAULT_SHORT = "new Short(Short.MIN_VALUE)";
    private static final String DEFAULT_INT = "new Integer(Integer.MIN_VALUE)";
    private static final String DEFAULT_LONG = "new Long(Long.MIN_VALUE)";
    private static final String DEFAULT_FLOAT = "new Float(Float.MIN_VALUE)";
    private static final String DEFAULT_DOUBLE = "new Double(Double.MIN_VALUE)";

    private static final String IN = "es.bsc.mobile.annotations.Parameter.Direction.IN";
    private static final String OUT = "es.bsc.mobile.annotations.Parameter.Direction.OUT";
    private static final String INOUT = "es.bsc.mobile.annotations.Parameter.Direction.INOUT";

    public MethodEditor(Method[] ceiMethods) {
        this.ceiMethods = ceiMethods;
    }

    //Edits Constructor invocations
    @Override
    public void edit(NewExpr ne) throws CannotCompileException {
        StringBuilder newExpre = new StringBuilder();
        StringBuilder params = new StringBuilder();
        try {
            StringBuilder log = new StringBuilder("Replacing object creation: new " + ne.getClassName() + "(");
            CtClass[] paramTypes = ne.getConstructor().getParameterTypes();
            int i = 1;
            for (CtClass parType : paramTypes) {
                if (i > 1) {
                    log.append(",");
                    params.append(",");
                }
                log.append(parType.getName());
                if (parType.isPrimitive()) {
                    params.append("$").append(i);
                } else {
                    String cast = "(" + parType.getName() + ")";
                    params.append("(").append(cast).append(RUNTIME).append(".newObjectAccess($").append(i).append(", true))");
                }
                i++;
            }
            log.append(")");
            LOGGER.log(Level.FINE, log.toString());
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }
        newExpre.append("$_ = $proceed(").append(params.toString()).append(");");
        LOGGER.log(Level.FINEST, "Replacing call by " + newExpre.toString());
        ne.replace(newExpre.toString());
    }

    @Override
    public void edit(MethodCall mc) throws CannotCompileException {
        Method ceiMethod = null;
        CtMethod calledMethod = null;
        try {
            calledMethod = mc.getMethod();
            ceiMethod = InstrumentationAssistant.checkRemote(calledMethod, ceiMethods);
        } catch (Exception e) {
            throw new CannotCompileException(e);
        }

        if (ceiMethod != null) {
            replaceTaskCreation(mc, calledMethod, ceiMethod);
        } else if (!InstrumentationAssistant.isEdited(calledMethod)) {
            blackBoxCall(mc);
        }
    }

    private void replaceTaskCreation(MethodCall mc, CtMethod calledMethod, Method ceiMethod)
            throws CannotCompileException {
        String methodName = mc.getMethodName();
        Class<?> retType = ceiMethod.getReturnType();
        boolean isVoid = retType.equals(void.class);
        boolean isStatic = Modifier.isStatic(calledMethod.getModifiers());
        Class<?>[] ceiParamTypes = ceiMethod.getParameterTypes();
        int numParams = ceiParamTypes.length;
        if (!isStatic) {
            numParams++;
        }
        if (!isVoid) {
            numParams++;
        }
        String methodClass = mc.getClassName();
        LOGGER.log(Level.FINE, "Replacing call to method " + methodClass + "." + methodName + ".");
        StringBuilder executeTask = new StringBuilder();
        executeTask.append(RUNTIME).append(".executeTask(");

        executeTask.append("\"").append(methodClass).append("\"").append(',');
        executeTask.append("\"").append(methodName).append("\"").append(',');
        executeTask.append(!isStatic).append(',');
        if (numParams == 0) {
            executeTask.append("new Object[0]);");
        } else {
            executeTask.append("new Object[]{");
            Annotation[][] ceiParamAnnot = ceiMethod.getParameterAnnotations();
            for (int i = 0; i < ceiParamAnnot.length; i++) {
                Class<?> formalType = ceiParamTypes[i];
                es.bsc.mobile.annotations.Parameter.Type annotType = ((es.bsc.mobile.annotations.Parameter) ceiParamAnnot[i][0]).type();
                String direction;
                String type;
                String value;
                if (formalType.isPrimitive()) {
                    if (formalType.equals(boolean.class)) {
                        type = BOOLEAN;
                        value = "new Boolean($" + (i + 1) + ")";
                    } else if (formalType.equals(char.class)) {
                        type = CHAR;
                        value = "new Character($" + (i + 1) + ")";
                    } else if (formalType.equals(byte.class)) {
                        type = BYTE;
                        value = "new Byte($" + (i + 1) + ")";
                    } else if (formalType.equals(short.class)) {
                        type = SHORT;
                        value = "new Short($" + (i + 1) + ")";
                    } else if (formalType.equals(int.class)) {
                        type = INT;
                        value = "new Integer($" + (i + 1) + ")";
                    } else if (formalType.equals(long.class)) {
                        type = LONG;
                        value = "new Long($" + (i + 1) + ")";
                    } else if (formalType.equals(float.class)) {
                        type = FLOAT;
                        value = "new Float($" + (i + 1) + ")";
                    } else {// formalType.equals(double.class)
                        type = DOUBLE;
                        value = "new Double($" + (i + 1) + ")";
                    }
                } else {
                    // Object, string or file
                    value = "$" + (i + 1);
                    if (formalType.equals(String.class)) {
                        if (annotType.equals(es.bsc.mobile.annotations.Parameter.Type.FILE)) {
                            type = FILE;
                        } else {
                            type = STRING;
                        }
                    } else {
                        type = OBJECT;
                    }
                }

                switch (((es.bsc.mobile.annotations.Parameter) ceiParamAnnot[i][0]).direction()) {
                    case IN:
                        direction = IN;
                        break;
                    case OUT:
                        direction = OUT;
                        break;
                    case INOUT:
                        direction = INOUT;
                        break;
                    default: // null
                        direction = IN;
                        break;
                }
                executeTask.append(createTaskParam(type, direction, value));
                if (i < ceiParamAnnot.length - 1) {
                    executeTask.append(",");
                }
            }

            // Add the target object of the call as an IN/INOUT parameter
            if (!isStatic) {
                if ((isVoid ? numParams : numParams - 1) > 1) {
                    executeTask.append(',');
                }
                executeTask.append(createTaskParam(OBJECT, INOUT, "$0"));
            }
            // Add the return value as an OUT parameter, if any
            StringBuilder afterExecute = new StringBuilder("");
            if (!isVoid) {
                if (numParams > 1) {
                    executeTask.append(',');
                }
                String typeName = retType.getName();
                if (retType.isPrimitive()) {
                    String tempRetVar = "ret" + System.nanoTime();
                    executeTask.append(createTaskParam(OBJECT, OUT, tempRetVar));

                    String returnValueClass;
                    String returnValueDefault;
                    String converterMethod = "";

                    if (typeName.equals(boolean.class.getName())) {
                        returnValueClass = "Boolean";
                        returnValueDefault = DEFAULT_BOOLEAN;
                        converterMethod = "booleanValue()";
                    } else if (typeName.equals(char.class.getName())) {
                        returnValueClass = "Character";
                        returnValueDefault = DEFAULT_CHAR;
                        converterMethod = "charValue()";
                    } else if (typeName.equals(byte.class.getName())) {
                        returnValueClass = "Byte";
                        returnValueDefault = DEFAULT_BYTE;
                        converterMethod = "byteValue()";
                    } else if (typeName.equals(short.class.getName())) {
                        returnValueClass = "Short";
                        returnValueDefault = DEFAULT_SHORT;
                        converterMethod = "shortValue()";
                    } else if (typeName.equals(int.class.getName())) {
                        returnValueClass = "Integer";
                        returnValueDefault = DEFAULT_INT;
                        converterMethod = "intValue()";
                    } else if (typeName.equals(long.class.getName())) {
                        returnValueClass = "Long";
                        returnValueDefault = DEFAULT_LONG;
                        converterMethod = "longValue()";
                    } else if (typeName.equals(float.class.getName())) {
                        returnValueClass = "Float";
                        returnValueDefault = DEFAULT_FLOAT;
                        converterMethod = "floatValue()";
                    } else { // (typeName.equals(double.class.getName()))
                        returnValueClass = "Double";
                        returnValueDefault = DEFAULT_DOUBLE;
                        converterMethod = "doubleValue()";
                    }
                    executeTask.insert(0, returnValueClass + " " + tempRetVar + " = " + returnValueDefault + ";");

                    //$_=(getValueAndRemove(var)).converterMethod();
                    afterExecute
                            .append("$_ = ((").append(returnValueClass).append(")").append(RUNTIME).append(".getValueAndRemove(").append(tempRetVar).append(")).").append(converterMethod).append(";");

                } else if (retType.isArray()) {
                    Class<?> compType = retType.getComponentType();
                    int numDim = typeName.lastIndexOf('[');
                    String dims = "[0]";
                    while (numDim-- > 0) {
                        dims += "[]";
                    }
                    while (compType.getComponentType() != null) {
                        compType = compType.getComponentType();
                    }
                    String compTypeName = compType.getName();
                    executeTask.insert(0, "$_ = new " + compTypeName + dims + ';');
                    executeTask.append(createTaskParam(OBJECT, OUT, "$_"));
                } else { // Object
                    if (typeName.equals(Boolean.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_BOOLEAN + ";");
                    } else if (typeName.equals(Character.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_CHAR + ";");
                    } else if (typeName.equals(Byte.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_BYTE + ";");
                    } else if (typeName.equals(Short.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_SHORT + ";");
                    } else if (typeName.equals(Integer.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_INT + ";");
                    } else if (typeName.equals(Long.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_LONG + ";");
                    } else if (typeName.equals(Float.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_FLOAT + ";");
                    } else if (typeName.equals(Double.class.getName())) {
                        executeTask.insert(0, "$_ = " + DEFAULT_DOUBLE + ";");
                    } else {
                        // Object (maybe String): use the no-args constructor
                        executeTask.insert(0, "$_ = new " + typeName + "();");
                    }
                    executeTask.append(createTaskParam(OBJECT, OUT, "$_"));
                }

            }
            executeTask.append("});");
            executeTask.append(afterExecute);
        }
        LOGGER.log(Level.FINEST, executeTask.toString());
        mc.replace(executeTask.toString());
    }

    private String createTaskParam(String type, String direction, String value) {
        return type + "," + direction + "," + value;
    }

    private void blackBoxCall(MethodCall mc) throws CannotCompileException {
        StringBuilder modifiedCall = new StringBuilder();

        StringBuilder originalCall = new StringBuilder();
        originalCall.append(mc.getClassName()).append(".").append(mc.getMethodName()).append("(");
        // Check if the black-box we're going to is one of the array watch methods
        boolean isArrayWatch = mc.getClassName().equals(ARRAY_ACCESS_WATCHER);

        StringBuilder callPars = new StringBuilder("(");
        try {
            CtClass[] paramTypes = mc.getMethod().getParameterTypes();

            int i = 1;
            for (CtClass parType : paramTypes) {
                if (i > 1) {
                    callPars.append(',');
                    originalCall.append(",");
                }
                originalCall.append(parType.getName());
                String parId = "$" + i;
                if (parType.isPrimitive()) {
                    callPars.append(parId);
                } else // Object (also array)
                 if (isArrayWatch && i == 3) {
                        callPars.append(parId);
                    } else {
                        callPars.append("(").append(parType.getName()).append(")")
                                .append(RUNTIME).append(".newObjectAccess(").append(parId).append(",true)");
                    }
                i++;

            }
            callPars.append(')');
            originalCall.append(")");
            LOGGER.log(Level.FINE, "Replacing black box call to method " + originalCall.toString());
            if (Modifier.isStatic(mc.getMethod().getModifiers())) {
                modifiedCall.append("$_ = $proceed").append(callPars.toString()).append(";");
            } else {
                if (mc.isSuper()) {
                } else {
                    modifiedCall.append("$0 = (").append(mc.getClassName()).append(")es.bsc.mobile.runtime.Runtime.newObjectAccess($0, true);");
                }
                modifiedCall.append("$_ = $proceed").append(callPars.toString()).append(";");

            }

            LOGGER.log(Level.FINEST, modifiedCall.toString());
            mc.replace(modifiedCall.toString());
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }
    }

    @Override
    public void edit(FieldAccess fa) throws CannotCompileException {
        LOGGER.log(Level.FINE, "Replacing Field access on object " + fa.getClassName() + "." + fa.getFieldName());
        CtField field = null;
        try {
            field = fa.getField();
            if (Modifier.isStatic(field.getModifiers())) {
                return;
            }
        } catch (NotFoundException e) {
            throw new CannotCompileException(e);
        }

        String fieldName = field.getName();
        boolean isWriter = fa.isWriter();
        String objectClass = fa.getClassName();

        // First check the object containing the field
        StringBuilder toInclude = new StringBuilder();
        toInclude.append(objectClass).append(" o = ").append(RUNTIME).append(".newObjectAccess($0,").append(isWriter).append(");");

        // Execute the access on the internal object
        if (isWriter) {
            toInclude.append("o.").append(fieldName).append(" = $1;"); // store a new value in the field
        } else {
            toInclude.append("$_ = o.").append(fieldName).append(';'); // read the field value
        }
        LOGGER.log(Level.FINEST, toInclude.toString());
        fa.replace(toInclude.toString());
    }

}
