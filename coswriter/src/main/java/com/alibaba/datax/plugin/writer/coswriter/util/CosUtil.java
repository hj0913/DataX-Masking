package com.alibaba.datax.plugin.writer.coswriter.util;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.coswriter.Key;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;

/**
 * Created by admin on 2018/11/7 0007.
 */
public class CosUtil {

    /**
     * 初始化腾讯云对象存储
     * @param conf 配置文件
     * @return 对象存储客户端
     */
    public static COSClient initClient(Configuration conf){
        String secretId = conf.getString(Key.SECRETID);
        String secretKey = conf.getString(Key.SECRETKEY);
        String region = conf.getString(Key.REGION);
        COSCredentials cred = new BasicCOSCredentials(secretId,secretKey);
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        return new COSClient(cred,clientConfig);
    }
}

