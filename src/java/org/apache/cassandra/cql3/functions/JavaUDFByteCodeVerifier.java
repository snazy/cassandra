/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3.functions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Handle;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Verifies Java UDF byte code.
 * Checks for disallowed method calls (e.g. {@code Object.finalize()}),
 * additional code in the constructor,
 * use of {@code synchronized} blocks,
 * too many methods.
 */
public final class JavaUDFByteCodeVerifier
{

    public static final String JAVA_UDF_NAME = JavaUDF.class.getName().replace('.', '/');
    public static final String OBJECT_NAME = Object.class.getName().replace('.', '/');
    public static final String CTOR_SIG = "(Lcom/datastax/driver/core/DataType;[Lcom/datastax/driver/core/DataType;)V";

    private final Multimap<String, String> disallowedMethodCalls = HashMultimap.create();
    private final List<String> disallowedPackages = new ArrayList<>();

    public JavaUDFByteCodeVerifier()
    {
        addDisallowedMethodCall(OBJECT_NAME, "clone");
        addDisallowedMethodCall(OBJECT_NAME, "finalize");
        addDisallowedMethodCall(OBJECT_NAME, "notify");
        addDisallowedMethodCall(OBJECT_NAME, "notifyAll");
        addDisallowedMethodCall(OBJECT_NAME, "wait");
    }

    public JavaUDFByteCodeVerifier addDisallowedMethodCall(String clazz, String method)
    {
        disallowedMethodCalls.put(clazz, method);
        return this;
    }

    public JavaUDFByteCodeVerifier addDisallowedPackage(String pkg)
    {
        disallowedPackages.add(pkg);
        return this;
    }

    public ClassAndErrors verify(byte[] bytes)
    {
        ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);

        Set<String> errors = new TreeSet<>(); // it's a TreeSet for unit tests
        ClassVisitor classVisitor = new ClassVisitor(Opcodes.ASM5, classWriter)
        {
            public FieldVisitor visitField(int access, String name, String desc, String signature, Object value)
            {
                errors.add("field declared: " + name);
                return super.visitField(access, name, desc, signature, value);
            }

            public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions)
            {
                if ((access & Opcodes.ACC_SYNCHRONIZED) != 0)
                    errors.add("synchronized method " + name);

                MethodVisitor delegate = super.visitMethod(access, name, desc, signature, exceptions);
                if ("<init>".equals(name) && CTOR_SIG.equals(desc))
                {
                    if (Opcodes.ACC_PUBLIC != access)
                        errors.add("constructor not public");
                    // allowed constructor - JavaUDF(DataType returnDataType, DataType[] argDataTypes)
                    return new ConstructorVisitor(errors, delegate);
                }
                if ("executeImpl".equals(name) && "(ILjava/util/List;)Ljava/nio/ByteBuffer;".equals(desc))
                {
                    if (Opcodes.ACC_PROTECTED != access)
                        errors.add("executeImpl not protected");
                    // the executeImpl method - ByteBuffer executeImpl(int protocolVersion, List<ByteBuffer> params)
                    return new ExecuteImplVisitor(errors, delegate);
                }
                if ("<clinit>".equals(name))
                {
                    errors.add("static initializer declared");
                }
                else
                {
                    errors.add("not allowed method declared: " + name + desc);
                    return new ExecuteImplVisitor(errors, delegate);
                }
                return delegate;
            }

            public void visit(int version, int access, String name, String signature, String superName, String[] interfaces)
            {
                if (!JAVA_UDF_NAME.equals(superName))
                {
                    errors.add("class does not extend " + JavaUDF.class.getName());
                }
                if (access != (Opcodes.ACC_PUBLIC | Opcodes.ACC_FINAL | Opcodes.ACC_SUPER))
                {
                    errors.add("class not public final");
                }
                super.visit(version, access, name, signature, superName, interfaces);
            }

            public void visitInnerClass(String name, String outerName, String innerName, int access)
            {
                errors.add("class declared as inner class");
                super.visitInnerClass(name, outerName, innerName, access);
            }
        };

        ClassReader classReader = new ClassReader(bytes);
        classReader.accept(classVisitor, 0);

        byte[] newByteCode = classWriter.toByteArray();

        return new ClassAndErrors(newByteCode, errors);
    }

    private class ExecuteImplVisitor extends MethodVisitor
    {
        private final Set<String> errors;

        ExecuteImplVisitor(Set<String> errors, MethodVisitor delegate)
        {
            super(Opcodes.ASM5, delegate);
            this.errors = errors;
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            Collection<String> disallowed = disallowedMethodCalls.get(owner);
            if (disallowed != null && disallowed.contains(name))
            {
                errors.add("call to " + name + "()");
            }
            if (!JAVA_UDF_NAME.equals(owner))
            {
                for (String pkg : disallowedPackages)
                {
                    if (owner.startsWith(pkg))
                        errors.add("call to " + owner + '.' + name + "()");
                }
            }
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        public void visitLabel(Label label)
        {
            // The trick to detect long running UDFs, is to inject a test method call after each label like this:
            //      if (super.runtimeExceeded()) return super.RUNTIME_EXCEEDED;
            super.visitLabel(label);

            Label continueLabel = new Label();
            // Insert following Java code
            //     if (o.a.c.cql3.functions.JavaUDF.checkTimeout())
            //         return null;
            // using the following bytecode
            //    INVOKESTATIC org/apache/cassandra/cql3/functions/JavaUDF.checkTimeout ()Z
            super.visitMethodInsn(Opcodes.INVOKESTATIC, "org/apache/cassandra/cql3/functions/JavaUDF", "udfExecCall", "()Z", false);
            //    IFEQ continueLabel
            super.visitJumpInsn(Opcodes.IFEQ, continueLabel);
            //    ACONST_NULL
            super.visitInsn(Opcodes.ACONST_NULL);
            //    ARETURN
            super.visitInsn(Opcodes.ARETURN);

            // need a label to continue normal execution
            super.visitLabel(continueLabel);
        }

        public void visitFrame(int type, int nLocal, Object[] local, int nStack, Object[] stack)
        {
            super.visitFrame(type, nLocal, local, nStack, stack);
        }

        public void visitInsn(int opcode)
        {
            switch (opcode)
            {
                case Opcodes.MONITORENTER:
                case Opcodes.MONITOREXIT:
                    errors.add("use of synchronized");
                    break;
            }
            super.visitInsn(opcode);
        }
    }

    private static class ConstructorVisitor extends MethodVisitor
    {
        private final Set<String> errors;

        ConstructorVisitor(Set<String> errors, MethodVisitor delegate)
        {
            super(Opcodes.ASM5, delegate);
            this.errors = errors;
        }

        public void visitInvokeDynamicInsn(String name, String desc, Handle bsm, Object... bsmArgs)
        {
            errors.add("Use of invalid method instruction in constructor");
            super.visitInvokeDynamicInsn(name, desc, bsm, bsmArgs);
        }

        public void visitMethodInsn(int opcode, String owner, String name, String desc, boolean itf)
        {
            if (!(Opcodes.INVOKESPECIAL == opcode && JAVA_UDF_NAME.equals(owner) && "<init>".equals(name) && CTOR_SIG.equals(desc)))
            {
                errors.add("initializer declared");
            }
            super.visitMethodInsn(opcode, owner, name, desc, itf);
        }

        public void visitInsn(int opcode)
        {
            if (Opcodes.RETURN != opcode)
            {
                errors.add("initializer declared");
            }
            super.visitInsn(opcode);
        }
    }

    public static class ClassAndErrors
    {
        public final byte[] bytecode;
        public final Set<String> errors;

        ClassAndErrors(byte[] bytecode, Set<String> errors)
        {
            this.bytecode = bytecode;
            this.errors = errors;
        }
    }
}
