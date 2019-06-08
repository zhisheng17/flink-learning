package com.zhisheng.connectors.kudu.serde;

import com.zhisheng.connectors.kudu.connector.KuduRow;
import com.zhisheng.connectors.kudu.connector.KuduTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.Schema;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Stream;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class PojoSerDe<P> implements KuduSerialization<P>, KuduDeserialization<P> {


    private Class<P> clazz;

    public transient KuduTableInfo tableInfo;
    public transient Schema schema;


    public PojoSerDe(Class<P> clazz) {
        this.clazz = clazz;
    }

    @Override
    public PojoSerDe<P> withSchema(Schema schema) {
        this.schema = schema;
        return this;
    }

    @Override
    public KuduRow serialize(P object) {
        return mapTo(object);
    }

    private KuduRow mapTo(P object) {
        if (schema == null) throw new IllegalArgumentException("schema must be set to serialize");

        KuduRow row = new KuduRow(schema.getRowSize());

        for (Class<?> c = object.getClass(); c != null; c = c.getSuperclass()) {
            basicValidation(c.getDeclaredFields())
                    .forEach(cField -> {
                        try {
                            cField.setAccessible(true);
                            row.setField(schema.getColumnIndex(cField.getName()), cField.getName(), cField.get(object));
                        } catch (IllegalAccessException e) {
                            String error = String.format("Cannot get value for %s", cField.getName());
                            throw new IllegalArgumentException(error, e);
                        }
                    });
        }

        return row;
    }

    private Stream<Field> basicValidation(Field[] fields) {
        return Arrays.stream(fields)
                .filter(field -> schemaHasColumn(field.getName()))
                .filter(field -> !Modifier.isStatic(field.getModifiers()))
                .filter(field -> !Modifier.isTransient(field.getModifiers()));
    }

    private boolean schemaHasColumn(String field) {
        return schema.getColumns().stream().anyMatch(col -> StringUtils.equalsIgnoreCase(col.getName(),field));
    }

    @Override
    public P deserialize(KuduRow row) {
        return mapFrom(row);
    }

    private P mapFrom(KuduRow row) {
        P o = createInstance(clazz);

        for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
            Field[] fields = c.getDeclaredFields();

            basicValidation(fields)
                    .forEach(cField -> {
                        try {
                            cField.setAccessible(true);
                            Object value = row.getField(cField.getName());
                            if (value != null) {
                                if (cField.getType() == value.getClass()) {
                                    cField.set(o, value);
                                } else if (cField.getType() == Long.class && value.getClass() == Date.class) {
                                    cField.set(o, ((Date) value).getTime());
                                } else {
                                    cField.set(o, value);
                                }
                            }
                        } catch (IllegalAccessException e) {
                            String error = String.format("Cannot get value for %s", cField.getName());
                            throw new IllegalArgumentException(error, e);
                        }
                    });
        }

        return o;

    }

    private P createInstance(Class<P> clazz) {
        try {
            Constructor<P> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            String error = String.format("Cannot create instance for %s", clazz.getSimpleName());
            throw new IllegalArgumentException(error, e);
        }
    }

}
