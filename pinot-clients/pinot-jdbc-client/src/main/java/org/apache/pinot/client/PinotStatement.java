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
package org.apache.pinot.client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.pinot.client.base.AbstractBaseStatement;


public class PinotStatement extends AbstractBaseStatement {

  private static final String QUERY_FORMAT = "sql";
  private Connection _connection;
  private org.apache.pinot.client.Connection _session;
  private ResultSetGroup _resultSetGroup;
  private boolean _closed;

  public PinotStatement(PinotConnection connection) {
    _connection = connection;
    _session = connection.getSession();
    _closed = false;
  }

  @Override
  public void close()
      throws SQLException {
    _closed = true;
  }

  @Override
  protected void validateState()
      throws SQLException {
    if (isClosed()) {
      throw new SQLException("Statement is already closed!");
    }
  }

  @Override
  public ResultSet executeQuery(String sql)
      throws SQLException {
    validateState();
    try {
      Request request = new Request(QUERY_FORMAT, sql);
      _resultSetGroup = _session.execute(request);
      if (_resultSetGroup.getResultSetCount() == 0) {
        return new PinotResultSet();
      }
      return new PinotResultSet(_resultSetGroup.getResultSet(0));
    } catch (PinotClientException e) {
      throw new SQLException(String.format("Failed to execute query : %s", sql), e);
    }
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    validateState();
    return _connection;
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return _closed;
  }
}
