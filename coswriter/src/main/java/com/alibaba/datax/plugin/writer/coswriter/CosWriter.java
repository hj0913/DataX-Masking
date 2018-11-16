package com.alibaba.datax.plugin.writer.coswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.RetryUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.TextCsvWriterManager;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredWriter;
import com.alibaba.datax.plugin.writer.coswriter.util.CosUtil;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.CompleteMultipartUploadRequest;
import com.qcloud.cos.model.CompleteMultipartUploadResult;
import com.qcloud.cos.model.InitiateMultipartUploadRequest;
import com.qcloud.cos.model.InitiateMultipartUploadResult;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PartETag;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.UploadPartRequest;
import com.qcloud.cos.model.UploadPartResult;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.Upload;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by admin on 2018/11/7 0007.
 */
public class CosWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private COSClient cosClient = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
            this.cosClient = CosUtil.initClient(this.writerSliceConfig);
        }

        /**
         * 校验数据
         */
        private void validateParameter() {
            this.writerSliceConfig.getNecessaryValue(Key.SECRETID, CosWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.SECRETKEY, CosWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.SECRETKEY, CosWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.SECRETKEY, CosWriterErrorCode.REQUIRED_VALUE);
            this.writerSliceConfig.getNecessaryValue(Key.SECRETKEY, CosWriterErrorCode.REQUIRED_VALUE);

            String compress = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer
                    .Key.COMPRESS);

            if (StringUtils.isNoneBlank(compress)) {
                String errorMessage = String.format(
                        "COS写暂时不支持压缩, 该压缩配置项[%s]不起效用", compress);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(
                        CosWriterErrorCode.ILLEGAL_VALUE, errorMessage);
            }

            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);

        }

        @Override
        public void prepare() {
            LOG.info("begin do prepare...");
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String writeMode = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer
                    .Key.WRITE_MODE);

            //判断bucket是否存在
            if (!this.cosClient.doesBucketExist(bucket)) {
                // this.ossClient.createBucket(bucket);
                String errorMessage = String.format(
                        "您配置的bucket [%s] 不存在, 请您确认您的配置项.", bucket);
                LOG.error(errorMessage);
                throw DataXException.asDataXException(
                        CosWriterErrorCode.ILLEGAL_VALUE, errorMessage);

            }
            LOG.info(String.format("access control details [%s].", this.cosClient.getBucketAcl(bucket).toString()));

            if ("truncate".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode truncate, 开始清理 [%s] 下面以 [%s] 开头的Object",
                                bucket, object));

                // warn: 默认情况下，如果Bucket中的Object数量大于100，则只会返回100个Object
                while (true) {

                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, 数据写入Bucket [%s] 下, 写入相应Object的前缀为  [%s]",
                                bucket, object));
            } else if ("nonConflict".equals(writeMode)) {

            }

        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String object = this.writerSliceConfig.getString(Key.OBJECT);
            String bucket = this.writerSliceConfig.getString(Key.BUCKET);
            Set<String> allObjects = new HashSet<String>();

            try {
                List<COSObjectSummary> cosObjectListing = this.cosClient.listObjects(bucket).getObjectSummaries();
                for (COSObjectSummary objectSummary : cosObjectListing) {
                    allObjects.add(objectSummary.getKey());
                }
            } catch (CosServiceException cosClientException) {

            } catch (CosClientException cosServiceException) {

            }
            String objectSuffix;

            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same object name
                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullObjectName = null;
                objectSuffix = StringUtils.replace(
                        UUID.randomUUID().toString(), "-", "");
                fullObjectName = String.format("%s__%s", object, objectSuffix);
                while (allObjects.contains(fullObjectName)) {
                    objectSuffix = StringUtils.replace(UUID.randomUUID()
                            .toString(), "-", "");
                    fullObjectName = String.format("%s__%s", object,
                            objectSuffix);
                }
                allObjects.add(fullObjectName);
                splitedTaskConfig.set(Key.OBJECT, fullObjectName);

                LOG.info(String.format("splited write object name:[%s]",
                        fullObjectName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }
    }

    public static class Task extends Writer.Task {

        private static final Logger LOG = LoggerFactory.getLogger(Task.class);
        private COSClient cosClient;
        private Configuration writerSliceConfig;
        private String bucket;
        private String object;
        private String primaryColumn;
        private String nullFormat;
        private String encoding;
        private char fieldDelimiter;
        private String dateFormat;
        private DateFormat dateParse;
        private String fileFormat;
        private List<String> header;
        private Long maxFileSize;// MB
        private String suffix;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.cosClient = CosUtil.initClient(this.writerSliceConfig);
            this.bucket = this.writerSliceConfig.getString(Key.BUCKET);
            this.object = this.writerSliceConfig.getString(Key.OBJECT);
            this.primaryColumn = this.writerSliceConfig.getString(Key.PRIMARYCOLUMN);
            this.nullFormat = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.NULL_FORMAT);
            this.dateFormat = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT, null);
            if (StringUtils.isNotBlank(this.dateFormat)) {
                this.dateParse = new SimpleDateFormat(dateFormat);
            }
            this.encoding = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.ENCODING,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_ENCODING);
            this.fieldDelimiter = this.writerSliceConfig.getChar(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FIELD_DELIMITER,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_FIELD_DELIMITER);
            this.fileFormat = this.writerSliceConfig.getString(
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.FILE_FORMAT_TEXT);
            this.header = this.writerSliceConfig
                    .getList(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.HEADER,
                            null, String.class);
            this.maxFileSize = this.writerSliceConfig
                    .getLong(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.MAX_FILE_SIZE,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.MAX_FILE_SIZE);
            this.suffix = this.writerSliceConfig
                    .getString(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.SUFFIX,
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Constant.DEFAULT_SUFFIX);
            this.suffix = this.suffix.trim();// warn: need trim
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) throws IOException {
            Record record = null;
            Integer pColumn = Integer.valueOf(this.primaryColumn);
            InputStream inputStream = null;
            while ((record = lineReceiver.getFromReader()) != null) {
                try {
                    String s = null;
                    if (StringUtils.isNoneBlank(this.primaryColumn)) {
                        this.suffix = record.getColumn(pColumn).asString();
                    }
                    String currentObject = String.format("%s_%s", this.object, this.suffix);

                    int columnNumbers = record.getColumnNumber();
                    for (int i = 0; i < columnNumbers; i++) {
                        if (i == pColumn) {
                            continue;
                        } else {
                            s = record.getColumn(i).asString();
                        }
                    }

                    if (!StringUtils.isNoneBlank(s)) {
                        continue;
                    }
                    ExecutorService threadPool = Executors.newFixedThreadPool(32);
                    TransferManager transferManager = new TransferManager(cosClient, threadPool);
                    ObjectMetadata objectMetadata = new ObjectMetadata();
                    objectMetadata.setContentLength(s.length());
                    inputStream = new ByteArrayInputStream(s.getBytes());
                    PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucket, currentObject, inputStream,objectMetadata);
                    transferManager.upload(putObjectRequest);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    inputStream.close();
                }
            }
        }

        private void upload(Upload upload){
            try {
                upload.waitForUploadResult();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /*@Override
        public void startWrite(RecordReceiver lineReceiver) {
            // 设置每块字符串长度
            final long partSize = 1;
            long numberCacul = 1;
            final long maxPartNumber = numberCacul >= 1 ? numberCacul : 1;
            int objectRollingNumber = 0;
            //warn: may be StringBuffer->StringBuilder
            StringWriter sw = new StringWriter();
            StringBuffer sb = sw.getBuffer();
            UnstructuredWriter unstructuredWriter = TextCsvWriterManager
                    .produceUnstructuredWriter(this.fileFormat,
                            this.fieldDelimiter, sw);
            Record record = null;
            LOG.info(String.format(
                    "begin do write, each object maxFileSize: [%s]MB...",
                    maxPartNumber * 10));
            String currentObject = this.object;
            InitiateMultipartUploadRequest currentInitiateMultipartUploadRequest = null;
            InitiateMultipartUploadResult currentInitiateMultipartUploadResult = null;
            boolean gotData = false;
            List<PartETag> currentPartETags = null;
            // to do:
            // 可以根据currentPartNumber做分块级别的重试，InitiateMultipartUploadRequest多次一个currentPartNumber会覆盖原有
            int currentPartNumber = 1;
            try {
                System.out.println("this.primaryColumn:" + this.primaryColumn);


                int flag = 0;
                // warn
                boolean needInitMultipartTransform = true;
                while ((record = lineReceiver.getFromReader()) != null) {
                    if (flag == Integer.valueOf(primaryColumn)) {
                        continue;
                    }
                    System.out.println("-----------------------------------------------this.suffix:" + this.suffix);
                    if (StringUtils.isNoneBlank(this.primaryColumn)) {
                        this.suffix = record.getColumn(Integer.valueOf(this.primaryColumn)).asString();
                    }
                    System.out.println("-----------------------------------------------this.suffix:" + this.suffix);
                    gotData = true;
                    // init:begin new multipart upload
                    if (needInitMultipartTransform) {
                        if (objectRollingNumber == 0) {
                            if (StringUtils.isBlank(this.suffix)) {
                                currentObject = this.object;
                            } else {
                                currentObject = String.format("%s%s",
                                        this.object, this.suffix);
                            }
                        } else {
                            // currentObject is like(no suffix)
                            // myfile__9b886b70fbef11e59a3600163e00068c_1
                            if (StringUtils.isBlank(this.suffix)) {
                                currentObject = String.format("%s_%s",
                                        this.object, objectRollingNumber);
                            } else {
                                // or with suffix
                                // myfile__9b886b70fbef11e59a3600163e00068c_1.csv
                                currentObject = String.format("%s_%s%s",
                                        this.object, objectRollingNumber,
                                        this.suffix);
                            }
                        }
                        objectRollingNumber++;
                        currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                                this.bucket, currentObject);
                        currentInitiateMultipartUploadResult = this.cosClient
                                .initiateMultipartUpload(currentInitiateMultipartUploadRequest);
                        currentPartETags = new ArrayList<PartETag>();
                        LOG.info(String
                                .format("write to bucket: [%s] object: [%s] with oss uploadId: [%s]",
                                        this.bucket, currentObject,
                                        currentInitiateMultipartUploadResult
                                                .getUploadId()));

                        // each object's header
                        if (null != this.header && !this.header.isEmpty()) {
                            unstructuredWriter.writeOneRecord(this.header);
                        }
                        // warn
                        needInitMultipartTransform = false;
                        currentPartNumber = 1;
                    }

                    // write: upload data to current object
                    UnstructuredStorageWriterUtil.transportOneRecord(record,
                            this.nullFormat, this.dateParse,
                            this.getTaskPluginCollector(), unstructuredWriter);

                    if (sb.length() >= partSize) {
                        this.uploadOnePart(sw, currentPartNumber,
                                currentInitiateMultipartUploadResult,
                                currentPartETags, currentObject);
                        currentPartNumber++;
                        sb.setLength(0);
                    }

                    // save: end current multipart upload
                    if (currentPartNumber > maxPartNumber) {
                        LOG.info(String
                                .format("current object [%s] size > %s, complete current multipart upload and begin " +
                                                "new one",
                                        currentObject, currentPartNumber
                                                * partSize));
                        CompleteMultipartUploadRequest currentCompleteMultipartUploadRequest = new
                                CompleteMultipartUploadRequest(
                                this.bucket, currentObject,
                                currentInitiateMultipartUploadResult
                                        .getUploadId(), currentPartETags);
                        CompleteMultipartUploadResult currentCompleteMultipartUploadResult = this.cosClient
                                .completeMultipartUpload(currentCompleteMultipartUploadRequest);
                        LOG.info(String.format(
                                "final object [%s] etag is:[%s]",
                                currentObject,
                                currentCompleteMultipartUploadResult.getETag()));
                        // warn
                        needInitMultipartTransform = true;
                    }
                    flag++;
                }

                if (!gotData) {
                    LOG.info("Receive no data from the source.");
                    currentInitiateMultipartUploadRequest = new InitiateMultipartUploadRequest(
                            this.bucket, currentObject);
                    currentInitiateMultipartUploadResult = this.cosClient
                            .initiateMultipartUpload(currentInitiateMultipartUploadRequest);
                    currentPartETags = new ArrayList<PartETag>();
                    // each object's header
                    if (null != this.header && !this.header.isEmpty()) {
                        unstructuredWriter.writeOneRecord(this.header);
                    }
                }
                // warn: may be some data stall in sb
                if (0 < sb.length()) {
                    this.uploadOnePart(sw, currentPartNumber,
                            currentInitiateMultipartUploadResult,
                            currentPartETags, currentObject);
                }
                CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                        this.bucket, currentObject,
                        currentInitiateMultipartUploadResult.getUploadId(),
                        currentPartETags);
                CompleteMultipartUploadResult completeMultipartUploadResult = this.cosClient
                        .completeMultipartUpload(completeMultipartUploadRequest);
                LOG.info(String.format("final object etag is:[%s]",
                        completeMultipartUploadResult.getETag()));
            } catch (IOException e) {
                e.printStackTrace();
                // 脏数据UnstructuredStorageWriterUtil.transportOneRecord已经记录,header
                // 都是字符串不认为有脏数据
                throw DataXException.asDataXException(
                        CosWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                throw DataXException.asDataXException(
                        CosWriterErrorCode.Write_OBJECT_ERROR, e.getMessage());
            }
            LOG.info("end do write");
        }*/

        /**
         * 对于同一个UploadID，该号码不但唯一标识这一块数据，也标识了这块数据在整个文件内的相对位置。
         * 如果你用同一个part号码，上传了新的数据，那么OSS上已有的这个号码的Part数据将被覆盖。
         *
         * @throws Exception
         */
        private void uploadOnePart(
                final StringWriter sw,
                final int partNumber,
                final InitiateMultipartUploadResult initiateMultipartUploadResult,
                final List<PartETag> partETags, final String currentObject)
                throws Exception {
            final String encoding = this.encoding;
            final String bucket = this.bucket;
            final COSClient cosClient = this.cosClient;
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    byte[] byteArray = sw.toString().getBytes(encoding);
                    InputStream inputStream = new ByteArrayInputStream(
                            byteArray);
                    // 创建UploadPartRequest，上传分块
                    UploadPartRequest uploadPartRequest = new UploadPartRequest();
                    uploadPartRequest.setBucketName(bucket);
                    uploadPartRequest.setKey(currentObject);
                    uploadPartRequest.setUploadId(initiateMultipartUploadResult
                            .getUploadId());
                    uploadPartRequest.setInputStream(inputStream);
                    uploadPartRequest.setPartSize(byteArray.length);
                    uploadPartRequest.setPartNumber(partNumber);
                    UploadPartResult uploadPartResult = cosClient
                            .uploadPart(uploadPartRequest);
                    partETags.add(uploadPartResult.getPartETag());
                    LOG.info(String
                            .format("upload part [%s] size [%s] Byte has been completed.",
                                    partNumber, byteArray.length));
                    IOUtils.closeQuietly(inputStream);
                    return true;
                }
            }, 3, 1000L, false);
        }

        @Override
        public void destroy() {

        }

        @Override
        public void prepare() {

        }

        @Override
        public void post() {

        }

    }
}