package com.zhisheng.connectors.kudu.connector;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import java.util.List;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class KuduFilterInfo {
    private String column;
    private FilterType type;
    private Object value;

    private KuduFilterInfo() { }

    public KuduPredicate toPredicate(Schema schema) {
        return toPredicate(schema.getColumn(this.column));
    }
    public KuduPredicate toPredicate(ColumnSchema column) {
        KuduPredicate predicate;
        switch (this.type) {
            case IS_IN:
                predicate = KuduPredicate.newInListPredicate(column, (List<?>) this.value);
                break;
            case IS_NULL:
                predicate = KuduPredicate.newIsNullPredicate(column);
                break;
            case IS_NOT_NULL:
                predicate = KuduPredicate.newIsNotNullPredicate(column);
                break;
            default:
                predicate = predicateComparator(column);
                break;
        }
        return predicate;
    }

    private KuduPredicate predicateComparator(ColumnSchema column) {

        KuduPredicate.ComparisonOp comparison = this.type.comparator;

        KuduPredicate predicate;

        switch (column.getType()) {
            case STRING:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (String)this.value);
                break;
            case FLOAT:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Float)this.value);
                break;
            case INT8:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Byte)this.value);
                break;
            case INT16:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Short)this.value);
                break;
            case INT32:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Integer)this.value);
                break;
            case INT64:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Long)this.value);
                break;
            case DOUBLE:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Double)this.value);
                break;
            case BOOL:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (Boolean)this.value);
                break;
            case UNIXTIME_MICROS:
                Long time = (Long)this.value;
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, time*1000);
                break;
            case BINARY:
                predicate = KuduPredicate.newComparisonPredicate(column, comparison, (byte[])this.value);
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + column.getType());
        }
        return predicate;
    }

    public static class Builder {
        private KuduFilterInfo filter;

        private Builder(String column) {
            this.filter = new KuduFilterInfo();
            this.filter.column = column;
        }

        public static Builder create(String column) {
            return new Builder(column);
        }

        public Builder greaterThan(Object value) {
            return filter(FilterType.GREATER, value);
        }

        public Builder lessThan(Object value) {
            return filter(FilterType.LESS, value);
        }

        public Builder equalTo(Object value) {
            return filter(FilterType.EQUAL, value);
        }

        public Builder greaterOrEqualTo(Object value) {
            return filter(FilterType.GREATER_EQUAL, value);
        }

        public Builder lessOrEqualTo(Object value) {
            return filter(FilterType.LESS_EQUAL, value);
        }

        public Builder isNotNull() {
            return filter(FilterType.IS_NOT_NULL, null);
        }

        public Builder isNull() {
            return filter(FilterType.IS_NULL, null);
        }

        public Builder isIn(List<?> values) {
            return filter(FilterType.IS_IN, values);
        }

        public Builder filter(FilterType type, Object value) {
            this.filter.type = type;
            this.filter.value = value;
            return this;
        }

        public KuduFilterInfo build() {
            return filter;
        }
    }

    public enum FilterType {
        GREATER(KuduPredicate.ComparisonOp.GREATER),
        GREATER_EQUAL(KuduPredicate.ComparisonOp.GREATER_EQUAL),
        EQUAL(KuduPredicate.ComparisonOp.EQUAL),
        LESS(KuduPredicate.ComparisonOp.LESS),
        LESS_EQUAL(KuduPredicate.ComparisonOp.LESS_EQUAL),
        IS_NOT_NULL(null),
        IS_NULL(null),
        IS_IN(null);

        final KuduPredicate.ComparisonOp comparator;

        FilterType(KuduPredicate.ComparisonOp comparator) {
            this.comparator = comparator;
        }

    }
}
