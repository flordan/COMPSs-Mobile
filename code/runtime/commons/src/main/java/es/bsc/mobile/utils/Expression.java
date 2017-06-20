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

import java.lang.reflect.Array;
import java.nio.CharBuffer;
import java.util.HashMap;

public abstract class Expression {

    private static final char[] OPERATORS = new char[]{'+', '-', '*', '/', '^'};

    public static Expression newExpression(String expression) {

        int[] expressionId = new int[]{0};
        HashMap<String, Expression> registeredExpressions = new HashMap<>();
        return newExpression(expression, expressionId, registeredExpressions);
    }

    public abstract int evaluate(Object... params);

    private static Expression newExpression(String expression, int[] expressionId, HashMap<String, Expression> registeredExpressions) {
        CharBuffer cb = CharBuffer.wrap(expression);
        int openPars = 0;
        StringBuilder replacedExpression = new StringBuilder();
        StringBuilder subExpression = new StringBuilder();

        while (cb.hasRemaining()) {
            char c = cb.get();
            switch (c) {
                case ' ':
                    break;
                case '(':
                    if (openPars > 0) {
                        subExpression.append(c);
                    }
                    openPars++;
                    break;
                case ')':
                    if (openPars > 1) {
                        subExpression.append(c);
                    }
                    openPars--;
                    if (openPars == 0) {
                        String exprId = "expr" + expressionId[0];
                        replacedExpression.append(exprId);
                        Expression ne = newExpression(subExpression.toString());
                        registeredExpressions.put(exprId, ne);
                        expressionId[0]++;
                        subExpression = new StringBuilder();
                    }
                    break;
                default:
                    if (openPars > 0) {
                        subExpression.append(c);
                    } else {
                        replacedExpression.append(c);
                    }
            }
        }

        Expression e = null;

        for (int opIdx = 0; opIdx < OPERATORS.length && e == null; opIdx++) {
            e = checkOperation(replacedExpression.toString(), OPERATORS[opIdx], expressionId, registeredExpressions);
        }
        if (e == null) {
            e = registeredExpressions.get(expression);
        }
        if (e == null) {
            e = parseAtom(expression);
        }
        return e;
    }

    private static Expression checkOperation(String expression, char operator, int[] expressionId, HashMap<String, Expression> registeredExpressions) {
        int opPosition = expression.indexOf(operator);

        if (opPosition > 0) {
            return createOperation(operator,
                    newExpression(expression.substring(0, opPosition), expressionId, registeredExpressions),
                    newExpression(expression.substring(opPosition + 1), expressionId, registeredExpressions)
            );
        }
        return null;
    }

    private static Expression createOperation(char operator, Expression... operands) {
        if (operator == '^') {
            return new Power(operands);
        }
        if (operator == '/') {
            return new Division(operands);
        }
        if (operator == '*') {
            return new Multiplication(operands);
        }
        if (operator == '-') {
            return new Substraction(operands);
        }
        if (operator == '+') {
            return new Addition(operands);
        }
        return null;
    }

    private static Expression parseAtom(String value) {
        if (value.startsWith("par")) {
            int opPosition = value.indexOf(".");
            if (opPosition > 0) {
                String parName = value.substring(0, opPosition);
                String dimension = value.substring(opPosition + 1);
                return new ParameterDimension(parName, dimension);
            } else {
                return new ParameterValue(value);
            }
        } else {
            return new Scalar(Integer.parseInt(value));
        }
    }

    private static class Addition extends Expression {

        private final Expression[] operands;

        private Addition(Expression... operands) {
            this.operands = operands;
        }

        @Override
        public int evaluate(Object... params) {
            int op0 = operands[0].evaluate(params);
            int op1 = operands[1].evaluate(params);
            return op0 + op1;
        }
    }

    private static class Substraction extends Expression {

        private final Expression[] operands;

        private Substraction(Expression... operands) {
            this.operands = operands;
        }

        @Override
        public int evaluate(Object... params) {
            int op0 = operands[0].evaluate(params);
            int op1 = operands[1].evaluate(params);
            return op0 - op1;
        }
    }

    private static class Multiplication extends Expression {

        private final Expression[] operands;

        private Multiplication(Expression... operands) {
            this.operands = operands;
        }

        @Override
        public int evaluate(Object... params) {
            int op0 = operands[0].evaluate(params);
            int op1 = operands[1].evaluate(params);
            return op0 * op1;
        }
    }

    private static class Division extends Expression {

        private final Expression[] operands;

        private Division(Expression... operands) {
            this.operands = operands;
        }

        @Override
        public int evaluate(Object... params) {
            int op0 = operands[0].evaluate(params);
            int op1 = operands[1].evaluate(params);
            return op0 / op1;
        }
    }

    private static class Power extends Expression {

        private final Expression[] operands;

        public Power(Expression... operands) {
            this.operands = operands;
        }

        @Override
        public int evaluate(Object... params) {
            int val = 1;
            int base = operands[0].evaluate(params);
            int exponent = operands[1].evaluate(params);
            for (int i = 0; i < exponent; i++) {
                val *= base;
            }
            return val;
        }
    }

    private static class Scalar extends Expression {

        int value;

        private Scalar(int value) {
            this.value = value;
        }

        @Override
        public int evaluate(Object[] values) {
            return value;
        }

    }

    private static class ParameterDimension extends Expression {

        public enum Dimension {
            X, Y, Z
        }
        private final Dimension dim;
        private final int parIdx;

        private ParameterDimension(String parName, String dimension) {
            if (dimension.equals("x") || dimension.equals("X")) {
                dim = Dimension.X;
            } else if (dimension.equals("y") || dimension.equals("Y")) {
                dim = Dimension.Y;
            } else {
                dim = Dimension.Z;
            }
            parIdx = Integer.parseInt(parName.substring(3));
        }

        @Override
        public int evaluate(Object[] values) {
            Object value = values[parIdx];
            Class<?> valClass = value.getClass();
            if (valClass.isArray()) {
                if (dim == Dimension.X) {
                    return Array.getLength(value);
                } else {
                    value = Array.get(value, 0);
                    valClass = value.getClass();
                    if (valClass.isArray()) {
                        if (dim == Dimension.Y) {
                            return Array.getLength(value);
                        } else {
                            value = Array.get(value, 0);
                            valClass = value.getClass();
                            if (valClass.isArray()) {
                                return Array.getLength(value);
                            } else {
                                return 1;
                            }

                        }
                    } else {
                        return 1;
                    }
                }
            } else {
                return 1;
            }
        }
    }

    private static class ParameterValue extends Expression {

        private final int parIdx;
        private final int[] coords;

        private ParameterValue(String value) {
            String[] parts = value.split("\\[");
            coords = new int[parts.length - 1];
            parIdx = Integer.parseInt(parts[0].substring(3));
            for (int i = 1; i < parts.length; i++) {
                coords[i - 1] = Integer.parseInt(parts[i].substring(0, parts[i].length() - 1));
            }
        }

        @Override
        public int evaluate(Object[] values) {
            if (coords.length == 0) {
                return (int) values[parIdx];
            } else {
                Object value = values[parIdx];
                for (int dim = 0; dim < coords.length; dim++) {
                    Class<?> valClass = value.getClass();
                    if (valClass.isArray()) {
                        int position = coords[dim];
                        if (Array.getLength(value) < position) {
                            throw new EvaluationException("ArrayOutOfBounds");
                        }
                        value = Array.get(value, coords[dim]);
                    } else {
                        throw new EvaluationException("Array value does not correspond to an array");
                    }
                }
                return (Integer) value;
            }
        }
    }

    public static class EvaluationException extends RuntimeException {

        EvaluationException(String message) {
            super(message);
        }

    }
}
