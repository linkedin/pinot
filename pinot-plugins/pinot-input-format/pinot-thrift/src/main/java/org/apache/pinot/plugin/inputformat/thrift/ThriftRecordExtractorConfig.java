/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.thrift;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.data.readers.RecordExtractorConfig;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;


/**
 * Config for {@link ThriftRecordExtractor}
 */
public class ThriftRecordExtractorConfig implements RecordExtractorConfig {
  private Map<String, Integer> _fieldIds= new HashMap<>();

  @Override
  public void init(RecordReaderConfig readerConfig) {
    TBase tObject;
    try {
      Class<?> thriftClass =
          this.getClass().getClassLoader().loadClass(((ThriftRecordReaderConfig) readerConfig).getThriftClass());
      tObject = (TBase) thriftClass.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    int index = 1;
    TFieldIdEnum tFieldIdEnum;
    while ((tFieldIdEnum = tObject.fieldForId(index)) != null) {
      _fieldIds.put(tFieldIdEnum.getFieldName(), index);
      index++;
    }
  }

  @Override
  public void init(Map<String, String> decoderProps) {

  }

  public Map<String, Integer> getFieldIds() {
    return _fieldIds;
  }
}
