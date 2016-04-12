/**
 * Copyright 2015 StreamSets Inc.
 * <p/>
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.destination.sample;

import com.microsoft.azure.storage.blob.*;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.config.CsvHeader;
import com.streamsets.pipeline.config.CsvMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.delimited.DelimitedCharDataGenerator;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.zip.GZIPOutputStream;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;

import com.streamsets.stage.lib.sample.IoStreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This target is an example and does not actually write to any destination.
 */
public abstract class AzureBlobStorageTarget extends BaseTarget {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStorageTarget.class);

    public abstract String getConnectionString();
    public abstract String getAccount();
    public abstract String getAccessKey();
    public abstract String getDirectoryDelimiter();
    public abstract String getContainer();
    public abstract String getPath();
    public abstract boolean getCompress();
    public abstract DataFormat getDataFormat();
    public abstract DataGeneratorFormatConfig getDataGeneratorFormatConfig();

    private DataGeneratorFactory generatorFactory;

    private CloudBlobClient blobClient;
    private CloudBlobContainer blobContainer;

    private int fileCount = 0;

    private static final String GZIP_EXTENSION = ".gz";

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        Target.Context context = getContext();

        String connStr = getConnectionString();
        String account = getAccount();
        String accessKey = getAccessKey();
        String container = getContainer();
        String dirDelimiter = getDirectoryDelimiter();
        DataFormat dataFormat = getDataFormat();
        DataGeneratorFormatConfig dataGeneratorFormatConfig = getDataGeneratorFormatConfig();

        dataGeneratorFormatConfig.init(
                context,
                dataFormat,
                Groups.AZURE_STORAGE.name(),
                "dataGeneratorFormatConfig",
                issues
        );
        if (issues.size() == 0) {
            generatorFactory = dataGeneratorFormatConfig.getDataGeneratorFactory();
        }

        if (connStr == null && connStr.isEmpty() && account == null && account.isEmpty()) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "connectionString", Errors.AZURE_03
                    )
            );
        }

        // If connection string is not supplied, but account name is
        if (connStr != null && connStr.isEmpty() && account != null && !account.isEmpty()) {

            if (accessKey == null) {
                issues.add(
                        getContext().createConfigIssue(
                                Groups.AZURE_STORAGE.name(), "accessKey", Errors.AZURE_03
                        )
                );
            }
            if (accessKey != null && accessKey.isEmpty()) {
                issues.add(
                        getContext().createConfigIssue(
                                Groups.AZURE_STORAGE.name(), "accessKey", Errors.AZURE_03
                        )
                );
            }
        }

        if (connStr.isEmpty()) {
            connStr = "DefaultEndpointsProtocol=http;"
                    + "AccountName=" + account + ";"
                    + "AccountKey=" + accessKey + ";"
                    + "BlobEndpoint=https://" + account + ".blob.core.windows.net/;"
                    + "TableEndpoint=https://" + account + ".table.core.windows.net/;"
                    + "QueueEndpoint=https://" + account + ".queue.core.windows.net/;"
                    + "FileEndpoint=https://" + account + ".file.core.windows.net/";
        }

        LOG.info("Azure Storage connection string: {}", connStr);

        CloudStorageAccount storageAccount = null;
        try {
            storageAccount = CloudStorageAccount.parse(connStr);
            LOG.info("Created cloud storage account");
            blobClient = storageAccount.createCloudBlobClient();
            LOG.info("Created blob client!");

            if (dirDelimiter != null && !dirDelimiter.isEmpty()) {
                blobClient.setDirectoryDelimiter(dirDelimiter);
                LOG.info("Set blob client directory delimiter: " + dirDelimiter);
            }

        }
        catch (IllegalArgumentException | URISyntaxException e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "connectionString", Errors.AZURE_02, e.getMessage()
                    )
            );
        }
        catch (InvalidKeyException e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "accessKey", Errors.AZURE_02, e.getMessage()
                    )
            );
        }
        catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "connectionString", Errors.AZURE_02, e.getMessage()
                    )
            );
        }

        try {
            blobContainer = blobClient.getContainerReference(container);

            LOG.debug("Creating container {}' if it doesn't already exist", container);
            blobContainer.createIfNotExists();

        }
        catch (URISyntaxException e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "container", Errors.AZURE_05, "Container: " + container + ", error: " + e.getMessage()
                    )
            );
            LOG.error("Invalid URI constructed container reference", e);
        }
        catch (StorageException e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "container", Errors.AZURE_06, e.getMessage()
                    )
            );
            LOG.error("Azure Storage exception in getting container reference", e);
        }
        catch (Exception e) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.AZURE_STORAGE.name(), "container", Errors.AZURE_04, e.getMessage()
                    )
            );
            LOG.error("Exception in getting container reference", e);
        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.

        super.destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void write(Batch batch) throws StageException {

        Iterator<Record> records = batch.getRecords();
        int writtenRecordCount = 0;
        DataGenerator generator;
        Record currentRecord;
        BlobOutputStream blobOutputStream = null;

        try {
            IoStreamUtils.ByRefByteArrayOutputStream bOut = new IoStreamUtils.ByRefByteArrayOutputStream();
            OutputStream out = bOut;

            // wrap with gzip compression output stream if required
            if (getCompress()) {
                out = new GZIPOutputStream(bOut);
            }

            generator = generatorFactory.getGenerator(out);
            while (records.hasNext()) {
                currentRecord = records.next();
                try {

                    generator.write(currentRecord);
                    writtenRecordCount++;

                } catch (IOException | StageException e) {
                    handleException(e, currentRecord);
                }
            }
            generator.close();

            LOG.info("Written records: " + writtenRecordCount);

            if (writtenRecordCount > 0) {
                fileCount++;
                StringBuilder fileName = new StringBuilder();
                fileName = fileName.append(getPath()).append(fileCount);

                LOG.info("Writing to Azure file path: " + fileName);

                if (getCompress()) {
                    fileName = fileName.append(GZIP_EXTENSION);
                }

                String blockPath = fileName.toString();

                CloudBlockBlob block = blobContainer.getBlockBlobReference(blockPath);
                LOG.info("Got reference to blob: {}", blockPath);

                // Avoid making a copy of the internal buffer maintained by the ByteArrayOutputStream by using
                // ByRefByteArrayOutputStream
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bOut.getInternalBuffer(), 0, bOut.size());

                blobOutputStream = block.openOutputStream();

                LOG.debug("Opened output stream to block blob");

                blobOutputStream.write(byteArrayInputStream, bOut.size());

                blobOutputStream.flush();
                blobOutputStream.close();

                LOG.debug("Wrote to output stream");
            }

        }
        catch (Exception e) {

            if(blobOutputStream != null){
                try{
                    blobOutputStream.close();
                }
                catch(Exception ex){
                    LOG.error(Errors.AZURE_08.getMessage(), e.toString(), e);
                    throw new StageException(Errors.AZURE_08, e.toString(), e);
                }
            }

            LOG.error(Errors.AZURE_08.getMessage(), e.toString(), e);
            throw new StageException(Errors.AZURE_08, e.toString(), e);
        }

    }

    private void handleException(Exception e, Record currentRecord) throws StageException {

        String recordSourceId = currentRecord.getHeader().getSourceId();

        LOG.debug("Handling exception {}, record source: {}", e.toString(), recordSourceId);
        switch (getContext().getOnErrorRecord()) {
            case DISCARD:
                LOG.debug("Discarding record");
                break;
            case TO_ERROR:
                getContext().toError(currentRecord, e);
                break;
            case STOP_PIPELINE:
                if (e instanceof StageException) {
                    throw (StageException) e;
                } else {
                    throw new StageException(Errors.AZURE_07, recordSourceId, e.toString(), e);
                }
            default:
                throw new IllegalStateException(Utils.format("Unknown OnErrorRecord option '{}'",
                        getContext().getOnErrorRecord()));
        }
    }

}
