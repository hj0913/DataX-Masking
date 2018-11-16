package com.alibaba.datax.common.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Administrator on 2016/11/22.
 */
public final class JacksonUtils {

    /**
     * LOGGER
     */
    protected static final Logger LOGGER = LoggerFactory.getLogger("JacksonUtils");

    /**
     * EXCEL Date 起始日
     */
    private static final int EXCEL_DATE_DAY_START = 30;

    /**
     * 工具类应隐藏public构造器
     */
    private JacksonUtils() {

    }

    /**
     * objectMapper
     */
    private static ObjectMapper objectMapper = new ObjectMapper();

    static {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        objectMapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        objectMapper.setDateFormat(sdf);
    }

    /**
     * 使用泛型方法，把json字符串转换为相应的JavaBean对象。
     * (1)转换为普通JavaBean：readValue(json,Student.class)
     * (2)转换为List,如List<Student>,将第二个参数传递为Student
     * [].class.然后使用Arrays.asList();方法把得到的数组转换为特定类型的List
     *
     * @param jsonStr
     * @param valueType
     * @return T
     */
    public static <T> T readValue(String jsonStr, Class<T> valueType) {
        try {
            return objectMapper.readValue(jsonStr, valueType);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * json数组转List
     *
     * @param jsonStr
     * @param valueTypeRef
     * @return T
     */
    public static <T> T readValue(String jsonStr, TypeReference<T> valueTypeRef) {
        try {
            return objectMapper.readValue(jsonStr, valueTypeRef);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * json数组转List
     *
     * @param jsonStr
     * @param javaType
     * @return T
     */
    public static <T> T readValue(String jsonStr, JavaType javaType) {
        try {
            return objectMapper.readValue(jsonStr, javaType);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *
     * @param type1 type1
     * @param type2 type2
     * @return JavaType
     */
    public static JavaType toJavaType(Class type1, Class type2) {
        return objectMapper.getTypeFactory().constructParametricType(type1, type2);
    }

    /**
     * @param module
     * @return ObjectMapper
     */
    public static synchronized ObjectMapper registerModule(Module module) {
        return objectMapper.registerModule(module);
    }

    /**
     * 把JavaBean转换为json字符串
     *
     * @param object
     * @return T
     */
    public static String toJSon(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @return String
     */
    public static String getString(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            String text = valueNode.asText();
            if (StringUtils.isEmpty(text)) {
                return null;
            } else {
                return clearNull(text, Charset.forName("UTF-8"));
            }
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @return String
     */
    public static String getArray(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            String text;
            if (valueNode.isArray()) {
                if (null != valueNode.get(0)) {
                    text = valueNode.get(0).asText();
                } else {
                    return valueNode.asText();
                }
            } else {
                text = valueNode.asText();
            }
            return clearNull(text, Charset.forName("UTF-8"));
        }
        return null;
    }


    /**
     * @param jsonNode
     * @param key
     * @return Double
     */
    public static Double getDouble(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            return valueNode.asDouble();
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @return Integer
     */
    public static Integer getInt(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            return valueNode.asInt();
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @param parsePatterns
     * @return Date
     */
    public static Date getDate(JsonNode jsonNode, String key, final String... parsePatterns) {
        JsonNode valueNode = jsonNode.get(key);
        Date date;
        if (valueNode != null) {
            String dateStr = valueNode.asText();
            if (dateStr == null) {
                return null;
            } else {
                dateStr = clearNull(dateStr, Charset.forName("UTF-8"));
                if (!StringUtils.isEmpty(dateStr)) {
                    try {
                        date = org.apache.commons.lang3.time.DateUtils.parseDate(dateStr.trim(), parsePatterns);
                    } catch (Exception e) {
                        LOGGER.info("Unable to parse the date: " + valueNode);
                        return null;
                    }
                    return date;
                }
            }
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @return Date
     */
    public static Date getLongToDate(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null && !StringUtils.isEmpty(valueNode.asText()) && !valueNode.asText().equals("null")) {
            valueNode = valueNode.get("$numberLong");
            if (valueNode == null) {
                return null;
            }
            Long l = valueNode.asLong();
            if (l == null) {
                return null;
            } else {
                return new Date(l);
            }
        }

        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @return Date
     */
    public static Date getCrawlTimeDate(JsonNode jsonNode, String key) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null && !valueNode.asText().equals("null")) {
            Long l = valueNode.get("$date").asLong();
            if (l == null) {
                return new Date();
            } else {
                return new Date(l);
            }
        }
        return null;
    }

    /**
     * @param jsonNode
     * @param key
     * @param parsePatterns
     * @return Date
     */
    public static Date getCrawlTime(JsonNode jsonNode, String key, String... parsePatterns) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            String timeStr = valueNode.asText();
            if (timeStr == null) {
                return new Date();
            } else {
                timeStr = clearNull(timeStr, Charset.forName("UTF-8"));
                if (timeStr.contains("+")) {
                    return null;
                }
                Date crawlTime;
                if (!StringUtils.isEmpty(timeStr)) {
                    try {
                        crawlTime = org.apache.commons.lang3.time.DateUtils.parseDate(timeStr.trim(), parsePatterns);
                    } catch (Exception e) {
                        return null;
                    }
                    return crawlTime;
                }
            }
        }
        return null;
    }

    /**
     * @param source
     * @param dest
     * @param <T>
     * @return <T>
     */
    public static <T> T copyProperties(Object source, Class<T> dest) {
        return JacksonUtils.readValue(JacksonUtils.toJSon(source), dest);
    }

    /**
     * @param jsonNode
     * @param key
     * @param locale
     * @param parsePatterns
     * @return Date
     */
    public static Date getDateByLocal(JsonNode jsonNode, String key, final Locale locale, final String...
            parsePatterns) {
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode != null) {
            String dateStr = valueNode.asText();
            if (dateStr == null) {
                return null;
            } else {
                dateStr = clearNull(dateStr, Charset.forName("UTF-8"));
                if (!StringUtils.isEmpty(dateStr)) {
                    return DateUtils.parseDateByPatternAndLocale(dateStr, locale, parsePatterns);
                }
            }
        }
        return null;
    }

    private static String clearNull(String toBeClear, Charset charset) {
        byte[] bytes = toBeClear.getBytes(charset);
        for (int i = 0; i < bytes.length; i++) {
            Byte b = bytes[i];
            if (b == 0x00) {
                bytes[i] = EXCEL_DATE_DAY_START;
            }
        }
        return new String(bytes, charset).trim();
    }
}
