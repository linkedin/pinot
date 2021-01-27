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
package org.apache.pinot.core.query.request.context.predicate;

import org.apache.pinot.spi.data.FieldSpec.DataType;


public abstract class BasePredicate implements Predicate {

  // Sometimes we can tell, during compile time, whether a predicate will always evaluate to true or false. In such
  // cases there is no need to evaluate the predicate during runtime. This field is null by default to indicate
  // that predicate does not have precomputed results.
  Boolean _precomputed = null;

  @Override
  public Boolean getPrecomputed() {
    return _precomputed;
  }

  @Override
  public void rewrite(DataType dataType) {
    // by default we assume that predicate will work with the specified data type.
  }
}
