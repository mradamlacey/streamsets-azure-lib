/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.stage.destination.sample;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import com.streamsets.stage.lib.sample.DataFormatChooserValues;

@StageDef(
    version = 1,
    label = "Azure Blob Storage",
    description = "",
    icon = "wasb.png",
    recordsByRef = true,
    onlineHelpRefUrl = ""
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class AzureBlobStorageDTarget extends AzureBlobStorageTarget {

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.STRING,
      defaultValue = "",
      label = "Connection String",
      displayPosition = 10,
      group = "AZURE_STORAGE"
  )
  public String connectionString;

  /** {@inheritDoc} */
  @Override
  public String getConnectionString() {
    return connectionString;
  }

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "Storage Account Name",
          displayPosition = 20,
          group = "AZURE_STORAGE"
  )
  public String account;

  /** {@inheritDoc} */
  @Override
  public String getAccount() {
    return account;
  }

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "Access Key",
          displayPosition = 30,
          group = "AZURE_STORAGE"
  )
  public String accessKey;

  /** {@inheritDoc} */
  @Override
  public String getAccessKey() {
    return accessKey;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "Container name",
          displayPosition = 40,
          group = "AZURE_STORAGE"
  )
  public String container;

  /** {@inheritDoc} */
  @Override
  public String getContainer() {
    return container;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "File path",
          displayPosition = 50,
          group = "AZURE_STORAGE"
  )
  public String path;

  /** {@inheritDoc} */
  @Override
  public String getPath() {
    return path;
  }

  @ConfigDef(
          required = false,
          type = ConfigDef.Type.STRING,
          defaultValue = "",
          label = "Directory Delimiter",
          displayPosition = 60,
          group = "AZURE_STORAGE"
  )
  public String directoryDelimiter;

  /** {@inheritDoc} */
  @Override
  public String getDirectoryDelimiter() {
    return directoryDelimiter;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.BOOLEAN,
          defaultValue = "false",
          label = "Compress with gzip",
          displayPosition = 70,
          group = "AZURE_STORAGE"
  )
  public boolean compress;

  /** {@inheritDoc} */
  @Override
  public boolean getCompress() {
    return compress;
  }

  @ConfigDef(
          required = true,
          type = ConfigDef.Type.MODEL,
          label = "Data Format",
          displayPosition = 80,
          group = "AZURE_STORAGE"
  )
  @ValueChooserModel(DataFormatChooserValues.class)
  public DataFormat dataFormat;

  @Override
  public DataFormat getDataFormat() {
    return dataFormat;
  }

  @ConfigDefBean(groups = {"AZURE_STORAGE"})
  public DataGeneratorFormatConfig dataGeneratorFormatConfig;

  @Override
  public DataGeneratorFormatConfig getDataGeneratorFormatConfig() {
    return dataGeneratorFormatConfig;
  }
}
