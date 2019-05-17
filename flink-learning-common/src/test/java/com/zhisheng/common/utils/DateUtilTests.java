package com.zhisheng.common.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtilTests {

    @Test
    public void testFormat() {
        Date date1 = new Date(1556665200000L);
        Assert.assertEquals(date1.getTime(), DateUtil.format(date1));

        Assert.assertEquals("2019-05-01", DateUtil.format(
                date1.getTime(), DateUtil.YYYY_MM_DD));
        Assert.assertEquals("2019-05-01", DateUtil.format(
                date1, DateTimeZone.getDefault(), DateUtil.YYYY_MM_DD));

        Assert.assertEquals(date1.getTime(), DateUtil.format(
                "2019-05-01", DateUtil.YYYY_MM_DD));
        Assert.assertEquals(1556723220000L, DateUtil.format(
                "2019-05-01 16:07", DateUtil.YYYY_MM_DD_HH_MM));
        Assert.assertEquals(1556723235000L, DateUtil.format(
                "2019-05-01 16:07:15", DateUtil.YYYY_MM_DD_HH_MM_SS));
        Assert.assertEquals(1556723235000L, DateUtil.format(
                "2019-05-01 16:07:15.0", DateUtil.YYYY_MM_DD_HH_MM_SS_0));
    }

    @Test
    public void testIsValidDate() {
        Assert.assertTrue(DateUtil.isValidDate(
                "2019-05-01", DateUtil.YYYY_MM_DD));

        Assert.assertFalse(DateUtil.isValidDate(
                "01-05-2019", DateUtil.YYYY_MM_DD));
    }

    @Test
    public void testToDate() {
        Assert.assertEquals(new Date(1556665200000L),
                DateUtil.toDate("2019-05-01", DateUtil.YYYY_MM_DD));
    }

    @Test
    public void testWithTimeAtStartOfDay() {
        DateTimeFormatter dtf =
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(1557874800000L);
        DateTime dt = new DateTime(1557874800000L);

        Assert.assertEquals("2019-05-15 00:00:00",
                DateUtil.withTimeAtStartOfDay(date, dtf));
        Assert.assertEquals("2019-05-15 00:00:00",
                DateUtil.withTimeAtStartOfDay(dt, dtf));
    }

    @Test
    public void testWithTimeAtEndOfDay() {
        DateTimeFormatter dtf =
                DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(1557961199000L);
        DateTime dt = new DateTime(1557961199000L);

        Assert.assertEquals("2019-05-15 23:59:59",
                DateUtil.withTimeAtEndOfDay(date, dtf));
        Assert.assertEquals("2019-05-15 23:59:59",
                DateUtil.withTimeAtEndOfDay(dt, dtf));
    }

    @Test
    public void testWithTimeAtStartOfNow() {
        Date date = new Date();
        date.setHours(0);
        date.setMinutes(0);
        date.setSeconds(0);
        String data = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        Assert.assertEquals(data, DateUtil.withTimeAtStartOfNow());
    }

    @Test
    public void testWithTimeAtEndOfNow() {
        Date date = new Date();
        date.setHours(23);
        date.setMinutes(59);
        date.setSeconds(59);
        String data = new SimpleDateFormat("yyyyMMddHHmmss").format(date);
        Assert.assertEquals(data, DateUtil.withTimeAtEndOfNow());
    }
}
