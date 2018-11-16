package com.alibaba.datax.plugin.writer.coswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by admin on 2018/11/7 0007.
 */
public enum CosWriterErrorCode implements ErrorCode {

    CONFIG_INVALID_EXCEPTION("CosWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("CosWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("CosWriter-02", "您填写的参数值不合法."),
    Write_OBJECT_ERROR("CosWriter-03", "您配置的目标Object在写入时异常."),
    OSS_COMM_ERROR("CosWriter-04", "执行相应的COS操作异常."),
    ;
    private final String code;
    private final String description;

    private CosWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
