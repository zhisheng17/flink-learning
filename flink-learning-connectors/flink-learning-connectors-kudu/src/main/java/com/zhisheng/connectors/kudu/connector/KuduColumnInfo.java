package com.zhisheng.connectors.kudu.connector;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

public class KuduColumnInfo implements Serializable {

    private String name;
    private Type type;
    private boolean key;
    private boolean rangeKey;
    private boolean hashKey;
    private boolean nullable;
    private Object defaultValue;
    private int blockSize;
    private Encoding encoding;
    private Compression compression;

    private KuduColumnInfo(String name, Type type) {
        this.name = name;
        this.type = type;
        this.blockSize = 0;
        this.key = false;
        this.rangeKey = false;
        this.hashKey = false;
        this.nullable = false;
        this.defaultValue = null;
        this.encoding = Encoding.AUTO;
        this.compression = Compression.DEFAULT;
    }

    protected String name() {
        return name;
    }

    protected boolean isRangeKey() {
        return rangeKey;
    }

    protected boolean isHashKey() {
        return hashKey;
    }

    protected ColumnSchema columnSchema() {
        return new ColumnSchema.ColumnSchemaBuilder(name, type)
                .key(key)
                .nullable(nullable)
                .defaultValue(defaultValue)
                .desiredBlockSize(blockSize)
                .encoding(encoding.encode)
                .compressionAlgorithm(compression.algorithm)
                .build();
    }

    public static class Builder {
        private KuduColumnInfo column;

        private Builder(String name, Type type) {
            this.column = new KuduColumnInfo(name, type);
        }

        public static Builder create(String name, Type type) {
            return new Builder(name, type);
        }

        public static Builder createByte(String name) {
            return create(name, Type.INT8);
        }

        public static Builder createShort(String name) {
            return create(name, Type.INT16);
        }

        public static Builder createInteger(String name) {
            return create(name, Type.INT32);
        }

        public static Builder createLong(String name) {
            return create(name, Type.INT64);
        }

        public static Builder createDouble(String name) {
            return create(name, Type.DOUBLE);
        }

        public static Builder createFloat(String name) {
            return create(name, Type.FLOAT);
        }

        public static Builder createString(String name) {
            return create(name, Type.STRING);
        }

        public static Builder createBool(String name) {
            return create(name, Type.BOOL);
        }

        public static Builder createByteArray(String name) {
            return create(name, Type.BINARY);
        }

        public static Builder createUnixTime(String name) {
            return create(name, Type.UNIXTIME_MICROS);
        }

        public Builder asKey() {
            return key(true);
        }

        public Builder key(boolean key) {
            this.column.key = key;
            return this;
        }

        public Builder asRangeKey() {
            return rangeKey(true);
        }

        public Builder rangeKey(boolean rangeKey) {
            this.column.rangeKey = rangeKey;
            return this;
        }

        public Builder asHashKey() {
            return hashKey(true);
        }

        public Builder hashKey(boolean hashKey) {
            this.column.hashKey = hashKey;
            return this;
        }

        public Builder asNullable() {
            return nullable(true);
        }

        public Builder asNotNullable() {
            return nullable(false);
        }

        public Builder nullable(boolean nullable) {
            this.column.nullable = nullable;
            return this;
        }

        public Builder defaultValue(Object defaultValue) {
            this.column.defaultValue = defaultValue;
            return this;
        }

        public Builder desiredBlockSize(int blockSize) {
            this.column.blockSize = blockSize;
            return this;
        }

        public Builder encoding(Encoding encoding) {
            this.column.encoding = encoding;
            return this;
        }

        public Builder compressionAlgorithm(Compression compression) {
            this.column.compression = compression;
            return this;
        }

        public KuduColumnInfo build() {
            return column;
        }
    }


    public enum Compression {
        UNKNOWN(ColumnSchema.CompressionAlgorithm.UNKNOWN),
        DEFAULT(ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION),
        WITHOUT(ColumnSchema.CompressionAlgorithm.NO_COMPRESSION),
        SNAPPY(ColumnSchema.CompressionAlgorithm.SNAPPY),
        LZ4(ColumnSchema.CompressionAlgorithm.LZ4),
        ZLIB(ColumnSchema.CompressionAlgorithm.ZLIB);

        final ColumnSchema.CompressionAlgorithm algorithm;

        Compression(ColumnSchema.CompressionAlgorithm algorithm) {
            this.algorithm = algorithm;
        }
    }

    public enum Encoding {
        UNKNOWN(ColumnSchema.Encoding.UNKNOWN),
        AUTO(ColumnSchema.Encoding.AUTO_ENCODING),
        PLAIN(ColumnSchema.Encoding.PLAIN_ENCODING),
        PREFIX(ColumnSchema.Encoding.PREFIX_ENCODING),
        GROUP_VARINT(ColumnSchema.Encoding.GROUP_VARINT),
        RLE(ColumnSchema.Encoding.RLE),
        DICT(ColumnSchema.Encoding.DICT_ENCODING),
        BIT_SHUFFLE(ColumnSchema.Encoding.BIT_SHUFFLE);

        final ColumnSchema.Encoding encode;

        Encoding(ColumnSchema.Encoding encode) {
            this.encode = encode;
        }
    }
}
