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
package org.apache.pinot.core.operator.transform.function;

import org.apache.pinot.spi.data.FieldSpec;


public class ArraySumTransformFunctionTest extends ArrayBaseTransformFunctionTest {

  @Override
  String getFunctionName() {
    return ArraySumTransformFunction.FUNCTION_NAME;
  }

  @Override
  Object getExpectResult(int[] intArrary) {
    long sumRes = 0;
    for (int v : intArrary) {
      sumRes += v;
    }
    return sumRes;
  }

  @Override
  Class getArrayFunctionClass() {
    return ArraySumTransformFunction.class;
  }

  @Override
  FieldSpec.DataType getResultDataType(FieldSpec.DataType inputDataType) {
    switch (inputDataType) {
      case INT:
      case LONG:
        return FieldSpec.DataType.LONG;
      case FLOAT:
      case DOUBLE:
        return FieldSpec.DataType.DOUBLE;
    }
    throw new IllegalArgumentException("Unsupported input data type - " + inputDataType);
  }
}
