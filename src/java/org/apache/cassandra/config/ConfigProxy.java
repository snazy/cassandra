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
package org.apache.cassandra.config;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.utils.FBUtilities;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

/**
 * A class that proxies configuration for read access via JMX.
 */
public class ConfigProxy implements DynamicMBean {
    private Config conf;

    public ConfigProxy(Config conf) {
        this.conf = conf;

        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final Map<String, Field> configFields = new HashMap<>();

    static {
        for (Field field : Config.class.getDeclaredFields())
        {
            if (Modifier.isStatic(field.getModifiers())) continue;
            configFields.put(field.getName(), field);
        }

        // CQL system.node_config table

        StringBuilder cqlCreateTable = new StringBuilder("CREATE TABLE ").append(Keyspace.SYSTEM_KS).append('.').append(SystemKeyspace.HOST_CONFIG_CF).
            append(" (\n  node inet,\n  ts timestamp,\n  reason text,");
        StringBuilder cqlInsert = new StringBuilder("INSERT INTO ").append(Keyspace.SYSTEM_KS).append('.').append(SystemKeyspace.HOST_CONFIG_CF).
            append(" (node, ts, reason");
        for (Field field : configFields.values())
        {
            Class<?> type = field.getType();
            String ctype = type.isArray() ? "list<"+ctype(type)+'>' : ctype(type);
            cqlCreateTable.append("\n  ").append(field.getName()).append(' ').append(ctype).append(',');
            cqlInsert.append(", ").append(field.getName());
        }

        INSERT_CQL_PARAMS = 3 + configFields.size();

        cqlInsert.append(") VALUES (?, ?, ?");
        for (int i=0; i<configFields.size(); i++)
            cqlInsert.append(", ?");
        cqlInsert.append(')');

        CREATE_CQL_TABLE = cqlCreateTable.append("\n  primary key (node, ts)\n) WITH CLUSTERING ORDER BY (ts DESC)\n").toString();
        INSERT_CQL = cqlInsert.toString();

        // MBean specific

        MBeanAttributeInfo[] attrInfos = new MBeanAttributeInfo[configFields.size()];
        int i=0;
        for (Field field : configFields.values()) {
            MBeanAttributeInfo attrInfo = new MBeanAttributeInfo(field.getName(), field.getType().getName(), null, true, false, false);
            attrInfos[i++] = attrInfo;
        }
        mbeanInfo = new MBeanInfo(Config.class.getName(), null, attrInfos, null, null, null);
    }

    // CQL system.node_config table specific

    public static final String CREATE_CQL_TABLE;
    public static final String INSERT_CQL;
    private static final int INSERT_CQL_PARAMS;

    private static String ctype(Class<?> type) {
        String ctype;
        if (type == String.class) ctype = "text";
        else if (type == Integer.class) ctype = "int";
        else if (type == int.class) ctype = "int";
        else if (type == Long.class) ctype = "bigint";
        else if (type == long.class) ctype = "bigint";
        else if (type == Boolean.class) ctype = "boolean";
        else if (type == boolean.class) ctype = "boolean";
        else if (type == Double.class) ctype = "double";
        else if (type == double.class) ctype = "double";
        else if (type.isEnum()) ctype = "text";
        else ctype = "text"; // use "Object.toString()" as default for now
        return ctype;
    }

    public Object[] cqlInsertParams(String reason) {
        Object[] params = new Object[INSERT_CQL_PARAMS];
        params[0] = FBUtilities.getBroadcastAddress();
        params[1] = new Date();
        params[2] = reason;
        int param = 3;
        try {
            for (Field field : configFields.values())
            {
                Object v = simpleTypeValue(field);
                params[param++] = v;
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return params;
    }

    private Object simpleTypeValue(Field field) throws IllegalAccessException {
        Class<?> type = field.getType();
        Object v = field.get(conf);
        if (type == String.class ||
            type == Integer.class || type == int.class ||
            type == Long.class || type == long.class ||
            type == Boolean.class || type == boolean.class ||
            type == Double.class || type == double.class)
        {}
        else if (type.isArray())
        {
            if (v != null) v = Arrays.asList((Object[]) v);
        }
        else
        {
            // use "Object.toString()" as default for now
            if (v != null) v = v.toString();
        }
        return v;
    }

    // MBean specific

    private static final String MBEAN_NAME = "org.apache.cassandra.cfg:type=Config";

    private static final MBeanInfo mbeanInfo;

    @Override public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
        try
        {
            Field f = configFields.get(attribute);
            return f != null ? simpleTypeValue(f) : null;
        }
        catch (IllegalAccessException e)
        {
            throw new ReflectionException(e);
        }
    }

    @Override public void setAttribute(Attribute attribute)
        throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
    }

    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList lst = new AttributeList(attributes.length);
        try
        {
            for (String attribute : attributes)
            {
                Field f = configFields.get(attribute);
                lst.add(f != null ? f.get(conf) : null);
            }
        }
        catch (IllegalAccessException e)
        {
            // TODO do what ??
        }
        return lst;
    }

    @Override public AttributeList setAttributes(AttributeList attributes) {
        return null;
    }

    @Override public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
        return null;
    }

    @Override public MBeanInfo getMBeanInfo() {
        return mbeanInfo;
    }
}
