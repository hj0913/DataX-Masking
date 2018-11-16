package com.alibaba.datax.common.util;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2017/6/3.
 */
public class DateUtils extends org.apache.commons.lang3.time.DateUtils {

    /**
     * 默认时间格式
     */
    public static final String DEFAULT_FORMAT = "yyyy-MM-dd HH:mm:ss";


    /**
     * 年月日时间格斯
     */
    public static final String YEARHOURDAY_FORMAT = "yyyy-MM-dd";


    /**
     * FastDateFormat
     */
    //public static final FastDateFormat CLEAN_FORMAT = FastDateFormat.getInstance("yyyy年MM月");

    public static final FastDateFormat DEFAULT_DATETIME_FORMAT  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * The UTC time zone (often referred to as GMT).
     * This is private as it is mutable.
     */
    private static final TimeZone UTC_TIME_ZONE = TimeZone.getTimeZone("GMT");
    /**
     * ISO8601 formatter for date-time without time zone.
     * The format used is <tt>yyyy-MM-dd'T'HH:mm:ss</tt>.
     */
    public static final FastDateFormat ISO_DATETIME_FORMAT
            = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss");

    /**
     * ISO8601 formatter for date-time with time zone.
     * The format used is <tt>yyyy-MM-dd'T'HH:mm:ssZZ</tt>.
     */
    public static final FastDateFormat ISO_DATETIME_TIME_ZONE_FORMAT
            = FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ssZZ");

    /**
     * ISO8601 formatter for date without time zone.
     * The format used is <tt>yyyy-MM-dd</tt>.
     */
    public static final FastDateFormat DATE_FORMAT_DAY
            = FastDateFormat.getInstance("yyyy-MM-dd");

    /**
     * ISO8601-like formatter for date with time zone.
     * The format used is <tt>yyyy-MM-ddZZ</tt>.
     * This pattern does not comply with the formal ISO8601 specification
     * as the standard does not allow a time zone  without a time.
     */
    public static final FastDateFormat ISO_DATE_TIME_ZONE_FORMAT
            = FastDateFormat.getInstance("yyyy-MM-ddZZ");

    /**
     * ISO8601 formatter for time without time zone.
     * The format used is <tt>'T'HH:mm:ss</tt>.
     */
    public static final FastDateFormat ISO_TIME_FORMAT
            = FastDateFormat.getInstance("'T'HH:mm:ss");

    /**
     * ISO8601 formatter for time with time zone.
     * The format used is <tt>'T'HH:mm:ssZZ</tt>.
     */
    public static final FastDateFormat ISO_TIME_TIME_ZONE_FORMAT
            = FastDateFormat.getInstance("'T'HH:mm:ssZZ");

    /**
     * ISO8601-like formatter for time without time zone.
     * The format used is <tt>HH:mm:ss</tt>.
     * This pattern does not comply with the formal ISO8601 specification
     * as the standard requires the 'T' prefix for times.
     */
    public static final FastDateFormat ISO_TIME_NO_T_FORMAT
            = FastDateFormat.getInstance("HH:mm:ss");

    /**
     * ISO8601-like formatter for time with time zone.
     * The format used is <tt>HH:mm:ssZZ</tt>.
     * This pattern does not comply with the formal ISO8601 specification
     * as the standard requires the 'T' prefix for times.
     */
    public static final FastDateFormat ISO_TIME_NO_T_TIME_ZONE_FORMAT
            = FastDateFormat.getInstance("HH:mm:ssZZ");

    /**
     * SMTP (and probably other) date headers.
     * The format used is <tt>EEE, dd MMM yyyy HH:mm:ss Z</tt> in US locale.
     */
    public static final FastDateFormat SMTP_DATETIME_FORMAT
            = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss Z", Locale.US);


    /**
     * 默认支持时间字符串格式类型
     */
    private static final String[] PATTERN_ARRAY = {
            DEFAULT_DATETIME_FORMAT.getPattern(),
            DATE_FORMAT_DAY.getPattern()
    };
     /**
     * ISO8601 formatter for date-time without time zone.
     * The format used is <tt>yyyy-MM-dd'T'HH:mm:ss</tt>.
     */
    public static final FastDateFormat DEFAULT_DATE_FORMAT_0 = FastDateFormat.getInstance("yyyy-MM-dd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_1  = FastDateFormat.getInstance("yyyy年MM月dd日");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_2  = FastDateFormat.getInstance("yyyy.MM.dd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_3  = FastDateFormat.getInstance("yyyy/MM/dd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_4  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_5  = FastDateFormat.getInstance("yyyMMdd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_6  = FastDateFormat.getInstance("yyyy、MM、dd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_7  = FastDateFormat.getInstance("yyyy－MM－dd");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_8  = FastDateFormat.getInstance("yyyy-MM-ddHH:mm:ss");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_9  = FastDateFormat.getInstance("yyyy.MM.dd.");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_10  = FastDateFormat.getInstance("yyyy年MM月");

    public static final FastDateFormat DEFAULT_DATE_FORMAT_11  = FastDateFormat.getInstance("yyyy-MM-dd HH:mm");
    public static final FastDateFormat[] CLEAN_FORMAT ={
            DEFAULT_DATE_FORMAT_0,
            DEFAULT_DATE_FORMAT_1,
            DEFAULT_DATE_FORMAT_2,
            DEFAULT_DATE_FORMAT_3,
            DEFAULT_DATE_FORMAT_4,
            DEFAULT_DATE_FORMAT_5,
            DEFAULT_DATE_FORMAT_6,
            DEFAULT_DATE_FORMAT_7,
            DEFAULT_DATE_FORMAT_8,
            DEFAULT_DATE_FORMAT_9,
            DEFAULT_DATE_FORMAT_10,
            DEFAULT_DATE_FORMAT_11,
    };
    /**
     * 数据清洗存在的时间字符串格式类型
     */
    public static final String[] CLEAN_PATTERN_ARRAY = {
            "yyyy-MM-dd",
            "yyyy年MM月dd日",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy.MM.dd",
            "yyyy/MM/dd",
            "yyyMMdd",
            "yyyy、MM、dd",
            "yyyy．MM．dd",
            "yyyy－MM－dd",
            "yyyy-MM-ddHH:mm:ss",
            "yyyy.MM.dd",
            "yyyy.MM.dd.",
            "yyyy年MM月",
            "yyyy-MM-dd HH:mm"

    };

    /**
     * 默认时间解析
     *
     * @param str 时间字符串
     * @return Date
     * @throws ParseException ParseException
     */
    public static Date parseDate(final String str) throws ParseException {
        return DateUtils.parseDate(str, DEFAULT_DATETIME_FORMAT.getPattern());
    }


    /**
     * @param str
     * @return Date
     * @throws ParseException ParseException
     */
    public static Date parseDateWithDefaultPattern(final String str) throws ParseException {
        return DateUtils.parseDate(str, DateUtils.DEFAULT_FORMAT, DateUtils.DATE_FORMAT_DAY.getPattern(), "yyyy/MM/dd"
                + " HH:mm:ss", "yyyy年MM月dd日", "yyyy/MM/dd", "yyyy.MM.dd", "yyyyMMdd");
    }

    /**
     * 默认Format
     *
     * @param date
     * @return String
     */
    public static String format(final Date date) {
        return DEFAULT_DATETIME_FORMAT.format(date);
    }

    /**
     * <p>Formats a date/time into a specific pattern using the UTC time zone.</p>
     *
     * @param millis  the date to format expressed in milliseconds
     * @param pattern the pattern to use to format the date, not null
     * @return the formatted date
     */
    public static String formatUTC(final long millis, final String pattern) {
        return format(new Date(millis), pattern, UTC_TIME_ZONE, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern using the UTC time zone.</p>
     *
     * @param date    the date to format, not null
     * @param pattern the pattern to use to format the date, not null
     * @return the formatted date
     */
    public static String formatUTC(final Date date, final String pattern) {
        return format(date, pattern, UTC_TIME_ZONE, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern using the UTC time zone.</p>
     *
     * @param millis  the date to format expressed in milliseconds
     * @param pattern the pattern to use to format the date, not null
     * @param locale  the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String formatUTC(final long millis, final String pattern, final Locale locale) {
        return format(new Date(millis), pattern, UTC_TIME_ZONE, locale);
    }

    /**
     * <p>Formats a date/time into a specific pattern using the UTC time zone.</p>
     *
     * @param date    the date to format, not null
     * @param pattern the pattern to use to format the date, not null
     * @param locale  the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String formatUTC(final Date date, final String pattern, final Locale locale) {
        return format(date, pattern, UTC_TIME_ZONE, locale);
    }

    /**
     * <p>Formats a date/time into a specific pattern.</p>
     *
     * @param millis  the date to format expressed in milliseconds
     * @param pattern the pattern to use to format the date, not null
     * @return the formatted date
     */
    public static String format(final long millis, final String pattern) {
        return format(new Date(millis), pattern, null, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern.</p>
     *
     * @param date    the date to format, not null
     * @param pattern the pattern to use to format the date, not null
     * @return the formatted date
     */
    public static String format(final Date date, final String pattern) {
        return format(date, pattern, null, null);
    }

    /**
     * <p>Formats a calendar into a specific pattern.</p>
     *
     * @param calendar the calendar to format, not null
     * @param pattern  the pattern to use to format the calendar, not null
     * @return the formatted calendar
     * @see FastDateFormat#format(Calendar)
     * @since 2.4
     */
    public static String format(final Calendar calendar, final String pattern) {
        return format(calendar, pattern, null, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a time zone.</p>
     *
     * @param millis   the time expressed in milliseconds
     * @param pattern  the pattern to use to format the date, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final long millis, final String pattern, final TimeZone timeZone) {
        return format(new Date(millis), pattern, timeZone, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a time zone.</p>
     *
     * @param date     the date to format, not null
     * @param pattern  the pattern to use to format the date, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final Date date, final String pattern, final TimeZone timeZone) {
        return format(date, pattern, timeZone, null);
    }

    /**
     * <p>Formats a calendar into a specific pattern in a time zone.</p>
     *
     * @param calendar the calendar to format, not null
     * @param pattern  the pattern to use to format the calendar, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @return the formatted calendar
     * @see FastDateFormat#format(Calendar)
     * @since 2.4
     */
    public static String format(final Calendar calendar, final String pattern, final TimeZone timeZone) {
        return format(calendar, pattern, timeZone, null);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a locale.</p>
     *
     * @param millis  the date to format expressed in milliseconds
     * @param pattern the pattern to use to format the date, not null
     * @param locale  the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final long millis, final String pattern, final Locale locale) {
        return format(new Date(millis), pattern, null, locale);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a locale.</p>
     *
     * @param date    the date to format, not null
     * @param pattern the pattern to use to format the date, not null
     * @param locale  the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final Date date, final String pattern, final Locale locale) {
        return format(date, pattern, null, locale);
    }

    /**
     * <p>Formats a calendar into a specific pattern in a locale.</p>
     *
     * @param calendar the calendar to format, not null
     * @param pattern  the pattern to use to format the calendar, not null
     * @param locale   the locale to use, may be <code>null</code>
     * @return the formatted calendar
     * @see FastDateFormat#format(Calendar)
     * @since 2.4
     */
    public static String format(final Calendar calendar, final String pattern, final Locale locale) {
        return format(calendar, pattern, null, locale);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a time zone  and locale.</p>
     *
     * @param millis   the date to format expressed in milliseconds
     * @param pattern  the pattern to use to format the date, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @param locale   the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final long millis, final String pattern, final TimeZone timeZone, final Locale locale) {
        return format(new Date(millis), pattern, timeZone, locale);
    }

    /**
     * <p>Formats a date/time into a specific pattern in a time zone  and locale.</p>
     *
     * @param date     the date to format, not null
     * @param pattern  the pattern to use to format the date, not null, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @param locale   the locale to use, may be <code>null</code>
     * @return the formatted date
     */
    public static String format(final Date date, final String pattern, final TimeZone timeZone, final Locale locale) {
        final FastDateFormat df = FastDateFormat.getInstance(pattern, timeZone, locale);
        return df.format(date);
    }

    /**
     * <p>Formats a calendar into a specific pattern in a time zone  and locale.</p>
     *
     * @param calendar the calendar to format, not null
     * @param pattern  the pattern to use to format the calendar, not null
     * @param timeZone the time zone  to use, may be <code>null</code>
     * @param locale   the locale to use, may be <code>null</code>
     * @return the formatted calendar
     * @see FastDateFormat#format(Calendar)
     * @since 2.4
     */
    public static String format(final Calendar calendar, final String pattern, final TimeZone timeZone, final Locale
            locale) {
        final FastDateFormat df = FastDateFormat.getInstance(pattern, timeZone, locale);
        return df.format(calendar);
    }

    /**
     * @param paramString
     * @return s
     */
    public static String getSystemDateByformart(String paramString) {
        Date now = new Date();
        FastDateFormat format = FastDateFormat.getInstance(paramString);
        return format.format(now);
    }

    /**
     * 按照pattern 格式解析 date字符串
     *
     * @param date    日期
     * @param pattern 格式
     * @return s
     * @throws ParseException
     */
    public static Date parseDateByPattern(String date, String pattern) {
        try {
            return DateUtils.parseDate(date, pattern);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 按照pattern 格式解析 date字符串
     *
     * @param date          日期
     * @param parsePatterns 格式
     * @return s
     * @throws ParseException
     */
    public static Date parseDateByPattern(String date, final String... parsePatterns) {
        try {
            return org.apache.commons.lang3.time.DateUtils.parseDate(date, parsePatterns);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * @param date
     * @param locale
     * @param parsePatterns parsePatterns
     * @return Date
     */
    public static Date parseDateByPatternAndLocale(String date, final Locale locale, final String... parsePatterns) {
        try {
            return org.apache.commons.lang3.time.DateUtils.parseDate(date, locale, parsePatterns);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 将日期转化成需求的格式字符串
     *
     * @param date    日期
     * @param pattern 格式
     * @return s
     */
    public static String getStringByDate(Date date, String pattern) {
        return DateUtils.format(date, pattern);
    }


    /**
     * @return s
     */
    public static String getSystemYYYYMMDD() {
        return DATE_FORMAT_DAY.format(new Date());
    }

    /**
     * @param time
     * @return s
     */
    public static Date parseTimeToDate(String time) {
        try {
            return DateUtils.parseDate(time, PATTERN_ARRAY);
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @return Date
     */
    public static Date getMidnight() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTime();
    }

    /**
     * 获取若干后月的时间
     *
     * @param month
     * @return String
     */
    public static String getMontTime(Integer month) {
        GregorianCalendar now = new GregorianCalendar();
        SimpleDateFormat fmtrq = new SimpleDateFormat("yyyy-MM-dd");
        DateFormat df = DateFormat.getDateInstance();
        now.add(GregorianCalendar.MONTH, month);   //可以是天数或月数  数字自定 -6前6个月
        String str = fmtrq.format(now.getTime());
        return str;
    }

    /**
     * @return Date
     */
    public static Date yesterdayMin() {
        Calendar cal = Calendar.getInstance();
        cal.add(cal.DATE, -1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 统一日期格式
     *
     * @param value
     * @return String
     */
    public static String getReplaceAllDate(String value) {
        StringBuffer sb = null;
        if (value != null && !"".equals(value)) {
            value = value.replaceAll("[（,）,(,),【,】,{,},<,>]", "");
            value = value.replaceAll("[:,：]", ":");
            value = value.replace("]", "");
            value = value.replace("[", "");
            value = value.trim();
            sb = new StringBuffer();
            sb.append(value);
        }
        return sb == null ? null : sb.toString();
    }


    /**
     * 格式化日期 各种格式转为
     * <p>
     * yyyyMMdd yyyy-MM-dd yyyy/MM/dd yyyy.MM.dd CST
     * <p>
     * yyyy年MM月dd日
     *
     * @param date
     * @return String
     * @throws ParseException
     */
    public static String formatDate(String date) {
        String result = null;
        SimpleDateFormat sf = null;
        try {

            if (null != date) {
                SimpleDateFormat format = new SimpleDateFormat("yyyy年MM月dd日");
                try {
                    format.parse(date);
                    return date;
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // yyyyMMdd
                String yyymmdd = "^[12]\\d{3}(0\\d|1[0-2])([0-2]\\d|3[01])$";

                // yyyy-MM-dd
                String yyyymmdd = "^[0-9]{4}-[0-9]{1,}-[0-9]{1,}$";

                // yyyy/MM/dd
                String yyyy = "^\\d{4}\\/\\d{1,}\\/\\d{1,}$";

                // yyyy.MM.dd
                String yyyyD = "^\\d{4}\\.\\d{1,}\\.\\d{1,}$";

                // yyyy
                String yyyYD = "^\\d{4}$";

                // yyyyMMdd 情况
                Pattern pattern = Pattern.compile(yyymmdd);
                Matcher matcher = pattern.matcher(date);
                if (matcher.matches()) {
                    sf = new SimpleDateFormat("yyyyMMdd");
                    Date d = sf.parse(date);
                    sf = null;
                    sf = new SimpleDateFormat("yyyy年MM月dd日");
                    result = sf.format(d);
                    sf = null;
                } else {
                    // yyyy-MM-dd 情况
                    pattern = Pattern.compile(yyyymmdd);
                    matcher = pattern.matcher(date);
                    if (matcher.matches()) {
                        sf = new SimpleDateFormat("yyyy-MM-dd");
                        Date d = sf.parse(date);
                        sf = null;
                        sf = new SimpleDateFormat("yyyy年MM月dd日");
                        result = sf.format(d);
                        sf = null;
                    } else {
                        // yyyy/MM/dd 情况
                        pattern = Pattern.compile(yyyy);
                        matcher = pattern.matcher(date);
                        if (matcher.matches()) {
                            sf = new SimpleDateFormat("yyyy/MM/dd");
                            Date d = sf.parse(date);
                            sf = null;
                            sf = new SimpleDateFormat("yyyy年MM月dd日");
                            result = sf.format(d);
                            sf = null;
                        } else {
                            // yyyy.MM.dd 情况
                            pattern = Pattern.compile(yyyyD);
                            matcher = pattern.matcher(date);
                            if (matcher.matches()) {
                                sf = new SimpleDateFormat("yyyy.MM.dd");
                                Date d = sf.parse(date);
                                sf = null;
                                sf = new SimpleDateFormat("yyyy年MM月dd日");
                                result = sf.format(d);
                                sf = null;
                            } else {
                                int indexXie = date.indexOf("/");
                                int indexMao = date.indexOf(":");
                                if (4 == indexXie && -1 != indexMao) {
                                    String newDate = date.substring(0, date.indexOf(" "));
                                    // yyyy/MM/dd hh:mm:ss 情况
                                    pattern = Pattern.compile(yyyy);
                                    matcher = pattern.matcher(newDate);
                                    if (matcher.matches()) {
                                        sf = new SimpleDateFormat("yyyy/MM/dd");
                                        Date d = sf.parse(newDate);
                                        sf = null;
                                        sf = new SimpleDateFormat("yyyy年MM月dd日");
                                        result = sf.format(d);
                                        sf = null;
                                    }
                                } else {
                                    int indexXie2 = date.indexOf("-");
                                    int indexMao2 = date.indexOf(":");
                                    if (4 == indexXie2 && -1 != indexMao2) {
                                        String newDate = "";
                                        if (-1 != date.indexOf(" ")) {
                                            newDate = date.substring(0, date.indexOf(" "));
                                        } else {
                                            newDate = date.replace(" ", "").substring(0, date.indexOf(":") - 2);
                                        }
                                        pattern = Pattern.compile(yyyymmdd);
                                        matcher = pattern.matcher(newDate);
                                        if (matcher.matches()) {
                                            sf = new SimpleDateFormat("yyyy-MM-dd");
                                            Date d = sf.parse(newDate);
                                            sf = null;
                                            sf = new SimpleDateFormat("yyyy年MM月dd日");
                                            result = sf.format(d);
                                            sf = null;
                                        } else {
                                            newDate = date.replace(" ", "").substring(0, date.indexOf(":") - 1);
                                            // yyyy-MM-dd hh:mm:ss 情况
                                            pattern = Pattern.compile(yyyymmdd);
                                            matcher = pattern.matcher(newDate);
                                            if (matcher.matches()) {
                                                sf = new SimpleDateFormat("yyyy-MM-dd");
                                                Date d = sf.parse(newDate);
                                                sf = null;
                                                sf = new SimpleDateFormat("yyyy年MM月dd日");
                                                result = sf.format(d);
                                                sf = null;
                                            } else {
                                                // 处理：Mon Apr 22 00:00:00 CST
                                                // 2013格式
                                                if (date.contains("CST")) {
                                                    sf = new SimpleDateFormat("yyyy年MM月dd日");
                                                    @SuppressWarnings("deprecation")
                                                    Date dt = new Date(date);
                                                    result = sf.format(dt);
                                                    sf = null;
                                                    dt = null;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return result;

    }
}
