package com.alibaba.datax.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by admin on 2018/11/9 0009.
 */
public class StringLengthTransformer extends Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(StringLengthTransformer.class);
    private Object masker;
    Integer key;
    int columnIndex;

    public StringLengthTransformer() {
        setTransformerName("dx_length");
        System.out.println("Using StringLength masker");
    }

    public Record evaluate(Record record, boolean[] flag, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_length transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (Integer) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        oriValue = oriValue.trim();
        if (StringUtils.isEmpty(oriValue)) {
            return record;
        }
        int oriValueLength = oriValue.length();
        if (oriValueLength >= key) {
            record.setColumn(columnIndex, new StringColumn(oriValue.substring(0, key)));
        }
        return record;
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        return null;
        /*try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_length transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (Integer) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        oriValue = oriValue.trim();
        if (StringUtils.isEmpty(oriValue)) {
            return record;
        }
        int oriValueLength = oriValue.length();
        if (oriValueLength >= key) {
            record.setColumn(columnIndex, new StringColumn(oriValue.substring(0, key)));
        }
        return record;*/
    }
}
