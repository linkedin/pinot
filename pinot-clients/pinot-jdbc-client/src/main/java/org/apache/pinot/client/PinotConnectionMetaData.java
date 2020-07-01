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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.apache.pinot.client.utils.DriverUtils;


public class PinotConnectionMetaData implements DatabaseMetaData {
  private final PinotConnection _connection;

  private static final String SYS_FUNCTIONS = "maxTimeuuid,minTimeuuid,now,token,uuid";
  private static final String NUM_FUNCTIONS = "avg,count,max,min,sum";
  private static final String TIME_FUNCTIONS = "toDate,toTimestamp,toUnixTimestamp,dateOf,unixTimestampOf";
  private static final String STRING_FUNCTIONS = "toDate,toTimestamp,toUnixTimestamp,dateOf,unixTimestampOf";

  public PinotConnectionMetaData(PinotConnection connection) {
    _connection = connection;
  }

  @Override
  public boolean allProceduresAreCallable()
      throws SQLException {
    return true;
  }

  @Override
  public boolean allTablesAreSelectable()
      throws SQLException {
    return true;
  }

  @Override
  public String getURL()
      throws SQLException {
    return DriverUtils.getURIFromBrokers(_connection.getSession().getBrokerList());
  }

  @Override
  public String getUserName()
      throws SQLException {
    return "";
  }

  @Override
  public boolean isReadOnly()
      throws SQLException {
    return true;
  }

  @Override
  public boolean nullsAreSortedHigh()
      throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedLow()
      throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart()
      throws SQLException {
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd()
      throws SQLException {
    return true;
  }

  @Override
  public String getDatabaseProductName()
      throws SQLException {
    return "APACHE_PINOT";
  }

  @Override
  public String getDatabaseProductVersion()
      throws SQLException {
    return "0.5";
  }

  @Override
  public String getDriverName()
      throws SQLException {
    return "APACHE_PINOT_DRIVER";
  }

  @Override
  public String getDriverVersion()
      throws SQLException {
    return "1.0";
  }

  @Override
  public int getDriverMajorVersion() {
    return 1;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
  }

  @Override
  public boolean usesLocalFiles()
      throws SQLException {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesLowerCaseIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers()
      throws SQLException {
    return true;
  }

  @Override
  public String getIdentifierQuoteString()
      throws SQLException {
    return "\"";
  }

  @Override
  public String getSQLKeywords()
      throws SQLException {
    return "";
  }

  @Override
  public String getNumericFunctions()
      throws SQLException {
    return NUM_FUNCTIONS;
  }

  @Override
  public String getStringFunctions()
      throws SQLException {
    return "";
  }

  @Override
  public String getSystemFunctions()
      throws SQLException {
    return SYS_FUNCTIONS;
  }

  @Override
  public String getTimeDateFunctions()
      throws SQLException {
    return TIME_FUNCTIONS;
  }

  @Override
  public String getSearchStringEscape()
      throws SQLException {
    return "\\";
  }

  @Override
  public String getExtraNameCharacters()
      throws SQLException {
    return "";
  }

  @Override
  public boolean supportsAlterTableWithAddColumn()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsColumnAliasing()
      throws SQLException {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType)
      throws SQLException {
    //TODO: Implement conversion mapping
    return true;
  }

  @Override
  public boolean supportsTableCorrelationNames()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupBy()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsNonNullableColumns()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOuterJoins()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins()
      throws SQLException {
    return true;
  }

  @Override
  public String getSchemaTerm()
      throws SQLException {
    return "table";
  }

  @Override
  public String getProcedureTerm()
      throws SQLException {
    return "functions";
  }

  @Override
  public String getCatalogTerm()
      throws SQLException {
    return "";
  }

  @Override
  public boolean isCatalogAtStart()
      throws SQLException {
    return true;
  }

  @Override
  public String getCatalogSeparator()
      throws SQLException {
    return ".";
  }

  @Override
  public boolean supportsSchemasInDataManipulation()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedDelete()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsUnion()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsUnionAll()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback()
      throws SQLException {
    return true;
  }

  @Override
  public int getMaxBinaryLiteralLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnNameLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxColumnsInTable()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxConnections()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCursorNameLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxIndexLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength()
      throws SQLException {
    return 0;
  }

  @Override
  public int getMaxRowSize()
      throws SQLException {
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs()
      throws SQLException {
    return true;
  }

  @Override
  public int getMaxStatementLength()
      throws SQLException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMaxStatements()
      throws SQLException {
    return 65535;
  }

  @Override
  public int getMaxTableNameLength()
      throws SQLException {
    return 100;
  }

  @Override
  public int getMaxTablesInSelect()
      throws SQLException {
    return 10;
  }

  @Override
  public int getMaxUserNameLength()
      throws SQLException {
    return 10;
  }

  @Override
  public int getDefaultTransactionIsolation()
      throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsTransactions()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    return level == Connection.TRANSACTION_NONE;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly()
      throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit()
      throws SQLException {
    return true;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions()
      throws SQLException {
    return true;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
      String columnNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getSchemas()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getCatalogs()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getTableTypes()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
      String foreignCatalog, String foreignSchema, String foreignTable)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getTypeInfo()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public boolean supportsResultSetType(int type)
      throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    return type == ResultSet.TYPE_FORWARD_ONLY
        && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean ownDeletesAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean ownInsertsAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean othersDeletesAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean othersInsertsAreVisible(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean updatesAreDetected(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean deletesAreDetected(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean insertsAreDetected(int type)
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsBatchUpdates()
      throws SQLException {
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public Connection getConnection()
      throws SQLException {
    return _connection;
  }

  @Override
  public boolean supportsSavepoints()
      throws SQLException {
    return false;
  }

  @Override
  public boolean supportsNamedParameters()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsMultipleOpenResults()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsGetGeneratedKeys()
      throws SQLException {
    return true;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
      String attributeNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getResultSetHoldability()
      throws SQLException {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public int getDatabaseMajorVersion()
      throws SQLException {
    return 1;
  }

  @Override
  public int getDatabaseMinorVersion()
      throws SQLException {
    return 0;
  }

  @Override
  public int getJDBCMajorVersion()
      throws SQLException {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion()
      throws SQLException {
    return 0;
  }

  @Override
  public int getSQLStateType()
      throws SQLException {
    return sqlStateSQL;
  }

  @Override
  public boolean locatorsUpdateCopy()
      throws SQLException {
    return true;
  }

  @Override
  public boolean supportsStatementPooling()
      throws SQLException {
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime()
      throws SQLException {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax()
      throws SQLException {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets()
      throws SQLException {
    return true;
  }

  @Override
  public ResultSet getClientInfoProperties()
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
      String columnNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    return PinotResultSet.empty();
  }

  @Override
  public boolean generatedKeyAlwaysReturned()
      throws SQLException {
    return false;
  }

  @Override
  public <T> T unwrap(Class<T> iface)
      throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface)
      throws SQLException {
    return true;
  }

}
