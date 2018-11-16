package com.alibaba.datax.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;


/**
 * Created by admin on 2018/11/2 0002.
 */
public class DateTransformerCopy extends Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(DateTransformerCopy.class);

    private Object masker;
    String[] key;
    int columnIndex;

    public DateTransformerCopy() {
        setTransformerName("dx_date");
        System.out.println("Using date masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_date transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (String[]) Arrays.copyOfRange(paras,1,paras.length-1);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);

        String oriValue = column.asString();
        Date changeDate = null;
        oriValue = oriValue.trim();
        //如果值为空直接转换成null值 不为null就去掉前后端的空格
        if (StringUtils.isEmpty(oriValue)) {
            record.setColumn(columnIndex, new DateColumn(changeDate));
            return record;
        }

        for (String formatStr : key) {
            System.out.println(formatStr);
            try {
                FastDateFormat.getInstance(formatStr).parse(oriValue);
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (changeDate == null) {
        }
        record.setColumn(columnIndex, new DateColumn(changeDate));
        return record;
    }
}

