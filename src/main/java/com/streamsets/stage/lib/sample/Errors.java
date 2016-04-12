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
package com.streamsets.stage.lib.sample;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.GenerateResourceBundle;

@GenerateResourceBundle
public enum Errors implements ErrorCode {

  AZURE_00("A configuration is invalid because: {}"),
  AZURE_01("Specific reason writing record failed: {}"),

  AZURE_02("Unable to connect to storage account: {}"),

  AZURE_03("Must supply either connection string or account name/access key"),

  AZURE_04("Error in getting blob container: {}"),
  AZURE_05("Resource URI constructed is invalid: {}"),

  AZURE_06("Azure Storage exception: {}"),

  AZURE_07("Error serializing record: '{}': {}"),

  AZURE_08("Error writing to Azure Storage blob: {}"),

  SAMPLE_00("Sample error 0 {}"),
  SAMPLE_01("Sample error 1: {}")
  ;
  private final String msg;

  Errors(String msg) {
    this.msg = msg;
  }

  /** {@inheritDoc} */
  @Override
  public String getCode() {
    return name();
  }

  /** {@inheritDoc} */
  @Override
  public String getMessage() {
    return msg;
  }
}
