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
public class ClearSpaceTransformer extends Transformer {

    private Object masker;
    String key;
    int columnIndex;

    public ClearSpaceTransformer() {
        setTransformerName("dx_clearSpace");
        System.out.println("Using ClearSpace masker");
    }

    /**
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  transformer函数参数
     */
    public Record evaluate(Record record, boolean[] flag, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_clearSpace transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (String) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        oriValue = oriValue.trim();
        if (StringUtils.isEmpty(oriValue)) {
            return record;
        } else {
            record.setColumn(columnIndex, new StringColumn(oriValue.replace(key, "")));
        }
        return record;
    }
    @Override
    public Record evaluate(Record record, Object... paras) {
        /*try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_clearSpace transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = (String) paras[1];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        oriValue = oriValue.trim();
        if (StringUtils.isEmpty(oriValue)) {
            return record;
        } else {
            record.setColumn(columnIndex, new StringColumn(oriValue.replace(key, "")));
        }*/
        return record;
    }
}
