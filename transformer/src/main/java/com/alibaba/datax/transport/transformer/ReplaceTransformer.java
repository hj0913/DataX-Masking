package com.alibaba.datax.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;


/**
 * Created by admin on 2018/11/2 0002.
 */
public class ReplaceTransformer extends Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(ReplaceTransformer.class);

    private Object masker;
    String key;
    int columnIndex;

    public ReplaceTransformer() {
        setTransformerName("dx_string_replace");
        System.out.println("Using string_replace masker");
    }

    @Override
    public Record evaluate(Record record, boolean[] flag, Object... paras) {
        try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_string_replace transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = String.valueOf(paras[1]);
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" +
                    Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);

        String oriValue = column.asString();
        //如果值为空直接转换成null值 不为null就去掉前后端的空格
        if (StringUtils.isEmpty(oriValue)) {
            record.setColumn(columnIndex, new StringColumn(""));
        } else {
            record.setColumn(columnIndex, new StringColumn(oriValue.replaceAll(".",key)));
        }
        return record;
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        return null;
        /*try {
            if (paras.length < 2) {
                throw new RuntimeException("dx_date transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
            key = String.valueOf(paras[1]);
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
        for (int i = 1, length = paras.length; i < length; i++) {
            String formatStr = String.valueOf(paras[i]);
            try {
                changeDate = FastDateFormat.getInstance(formatStr).parse(oriValue);
                break;
            } catch (Exception e) {
                //LOG.error(e.getMessage());
                //这个是转换数据失败时，不用记录日志
            }
        }


        if (changeDate == null) {
            LOG.warn("transformer error data:{},record id:{},eid:{}", oriValue, record.getColumn(0).getRawData(),
                    record.getColumn(1).getRawData());
        }
        record.setColumn(columnIndex, new DateColumn(changeDate));
        return record;*/
    }
}

