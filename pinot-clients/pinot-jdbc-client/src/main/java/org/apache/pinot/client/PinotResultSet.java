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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.pinot.client.base.AbstractBaseResultSet;


public class PinotResultSet extends AbstractBaseResultSet {
  private static final String TIMESTAMP_FORMAT = "dd-mm-yyyy HH:MM:SS";
  private static final String DATE_FORMAT = "dd-mm-yyyy";
  private final SimpleDateFormat _dateFormat = new SimpleDateFormat(DATE_FORMAT);
  private final SimpleDateFormat _timestampFormat = new SimpleDateFormat(TIMESTAMP_FORMAT);

  private org.apache.pinot.client.ResultSet _resultSet;
  private int _totalRows;
  private int _currentRow;
  private int _totalColumns;
  private Map<String, Integer> _columns = new HashMap<>();

  public PinotResultSet(org.apache.pinot.client.ResultSet resultSet) {
    _resultSet = resultSet;
    _totalRows = _resultSet.getRowCount();
    _totalColumns = _resultSet.getColumnCount();
    _currentRow = -1;
    for (int i = 0; i < _totalColumns; i++) {
      _columns.put(_resultSet.getColumnName(i), i);
    }
  }

  protected void validateState()
      throws SQLException {
    if (_resultSet == null) {
      throw new SQLException("Not allowed to operate on closed result sets");
    }
  }

  protected void validateColumn(int columnIndex)
      throws SQLException {
    validateState();

    if (columnIndex > _totalColumns) {
      throw new SQLException("Column Index should be less than " + (_totalColumns + 1) + ". Found " + columnIndex);
    }
  }

  @Override
  public boolean absolute(int row)
      throws SQLException {
    validateState();

    if (row >= 0 && row < _totalRows) {
      _currentRow = row;
      return true;
    } else if (row < 0 && Math.abs(row) <= _totalRows) {
      _currentRow = _totalRows + row;
      return true;
    }

    return false;
  }

  @Override
  public void afterLast()
      throws SQLException {
    validateState();

    _currentRow = _totalRows;
  }

  @Override
  public void beforeFirst()
      throws SQLException {
    validateState();

    _currentRow = -1;
  }

  @Override
  public void close()
      throws SQLException {
    _resultSet = null;
    _totalRows = 0;
    _currentRow = -1;
    _columns.clear();
  }

  @Override
  public int findColumn(String columnLabel)
      throws SQLException {
    if (_columns.containsKey(columnLabel)) {
      return _columns.get(columnLabel);
    } else {
      throw new SQLException("Column with label " + columnLabel + " not found in ResultSet");
    }
  }

  @Override
  public boolean first()
      throws SQLException {
    validateState();

    _currentRow = 0;
    return true;
  }

  @Override
  public InputStream getAsciiStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.US_ASCII));
    return in;
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      BigDecimal bigDecimal = new BigDecimal(value).setScale(scale);
      return bigDecimal;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch BigDecimal value", e);
    }
  }

  @Override
  public boolean getBoolean(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);
    return Boolean.parseBoolean(_resultSet.getString(_currentRow, columnIndex - 1));
  }

  @Override
  public byte[] getBytes(int columnIndex)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      return Hex.decodeHex(value.toCharArray());
    } catch (Exception e) {
      throw new SQLException(String.format("Unable to fetch value for column %d", columnIndex), e);
    }
  }

  @Override
  public Reader getCharacterStream(int columnIndex)
      throws SQLException {
    InputStream in = getUnicodeStream(columnIndex);
    Reader reader = new InputStreamReader(in, StandardCharsets.UTF_8);
    return reader;
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      java.util.Date date = _dateFormat.parse(value);
      cal.setTime(date);
      Date sqlDate = new Date(cal.getTimeInMillis());
      return sqlDate;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public double getDouble(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);

    return _resultSet.getDouble(_currentRow, columnIndex - 1);
  }

  @Override
  public float getFloat(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);

    return _resultSet.getFloat(_currentRow, columnIndex - 1);
  }

  @Override
  public int getInt(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);

    return _resultSet.getInt(_currentRow, columnIndex - 1);
  }

  @Override
  public long getLong(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);

    return _resultSet.getLong(_currentRow, columnIndex - 1);
  }

  @Override
  public int getRow()
      throws SQLException {
    validateState();

    return _currentRow;
  }

  @Override
  public short getShort(int columnIndex)
      throws SQLException {
    Integer value = getInt(columnIndex);
    return value.shortValue();
  }

  @Override
  public String getString(int columnIndex)
      throws SQLException {
    validateColumn(columnIndex);

    return _resultSet.getString(_currentRow, columnIndex - 1);
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      java.util.Date date = _timestampFormat.parse(value);
      cal.setTime(date);
      Time sqlTime = new Time(cal.getTimeInMillis());
      return sqlTime;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    try {
      String value = getString(columnIndex);
      java.util.Date date = _timestampFormat.parse(value);
      cal.setTime(date);
      Timestamp sqlTime = new Timestamp(cal.getTimeInMillis());
      return sqlTime;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch date", e);
    }
  }

  @Override
  public URL getURL(int columnIndex)
      throws SQLException {
    try {
      URL url = new URL(getString(columnIndex));
      return url;
    } catch (Exception e) {
      throw new SQLException("Unable to fetch URL", e);
    }
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex)
      throws SQLException {
    String value = getString(columnIndex);
    InputStream in = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    return in;
  }

  @Override
  public boolean isAfterLast()
      throws SQLException {
    validateState();

    return (_currentRow >= _totalRows);
  }

  @Override
  public boolean isBeforeFirst()
      throws SQLException {
    validateState();

    return (_currentRow < 0);
  }

  @Override
  public boolean isClosed()
      throws SQLException {
    return false;
  }

  @Override
  public boolean isFirst()
      throws SQLException {
    validateState();

    return _currentRow == 0;
  }

  @Override
  public boolean isLast()
      throws SQLException {
    validateState();

    return _currentRow == _totalRows - 1;
  }

  @Override
  public boolean last()
      throws SQLException {
    validateState();

    _currentRow = _totalRows - 1;
    return true;
  }

  @Override
  public boolean next()
      throws SQLException {
    validateState();

    _currentRow++;
    boolean hasNext = _currentRow < _totalRows;
    return hasNext;
  }

  @Override
  public boolean previous()
      throws SQLException {
    validateState();

    if (!isBeforeFirst()) {
      _currentRow--;
      return true;
    }
    return false;
  }

  @Override
  public boolean relative(int rows)
      throws SQLException {
    validateState();
    int nextRow = _currentRow + rows;
    if (nextRow >= 0 && nextRow < _totalRows) {
      _currentRow = nextRow;
      return true;
    }
    return false;
  }

  @Override
  public boolean wasNull()
      throws SQLException {
    return false;
  }
}
