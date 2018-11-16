package com.alibaba.datax.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by admin on 2018/11/5 0005.
 */
public class CapiTransformer extends Transformer {

    private Object masker;
    String key;
    int columnIndex;

    public CapiTransformer() {
        setTransformerName("dx_capi");
        System.out.println("Using Capi masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("dx_capi transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            //key = String.valueOf(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();

        //如果值为空直接转换成null值 不为null就去掉前后端的空格
        if (StringUtils.isEmpty(oriValue)) {
            record.setColumn(columnIndex, null);
            return record;
        } else {
            oriValue = oriValue.trim();
        }
        try {
            Pattern pattern = Pattern.compile("[1-9]\\d*\\.?\\d*");
            Matcher matcher = pattern.matcher(oriValue);
            if (matcher.find()) {
                oriValue = matcher.group();
            } else {
                oriValue = "0.0";
            }
            record.setColumn(columnIndex, new DoubleColumn(oriValue));
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),
                    e);
        }

        return record;
    }
}
