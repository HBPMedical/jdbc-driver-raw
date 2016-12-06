package raw.jdbc;

import raw.jdbc.rawclient.RawRestClient;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public class RawDatabaseMetaData implements DatabaseMetaData {

    String url;
    String user;
    RawRestClient client;

    RawDatabaseMetaData(String url, String user, RawRestClient client) {
        this.url = url;
        this.user = user;
        this.client = client;
    }

    public boolean allProceduresAreCallable() throws SQLException {
        throw new UnsupportedOperationException("not implemented allProceduresAreCallable");
    }

    public boolean allTablesAreSelectable() throws SQLException {
        throw new UnsupportedOperationException("not implemented allTablesAreSelectable");
    }

    public String getURL() throws SQLException {
        return this.url;
    }

    public String getUserName() throws SQLException {
        return user;
    }

    public boolean isReadOnly() throws SQLException {
        return true;
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    public String getDatabaseProductName() throws SQLException {
        return "raw-labs DB";
    }

    public String getDatabaseProductVersion() throws SQLException {
        return "0.0.1";
    }

    public String getDriverName() throws SQLException {
        return "raw rest-jdbc bridge";
    }

    public String getDriverVersion() throws SQLException {
        return "0.0.1";
    }

    public int getDriverMajorVersion() {
        return 0;
    }

    public int getDriverMinorVersion() {

        return 1;
    }

    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    public String getIdentifierQuoteString() throws SQLException {
        return "";
    }

    public String getSQLKeywords() throws SQLException {
        return "select|distinct|from|where|group|by|having|in|union|order|desc|asc|" +
                "if|then|else|parse|parse?|into|not|and|or|flatten|like|" +
                "as|as?|all|cast|partition|on|error|fail|skip|isnull|isnone|collection";
    }

    public String getNumericFunctions() throws SQLException {
        return "avg|count|max|min|min?|max?|sum";
    }

    public String getStringFunctions() throws SQLException {
        return "trim|startswith|strempty|strtodate|strtodate?|" +
                "strtotime|strtotime?|strtotimestamp|strtotimestamp?";
    }

    public String getSystemFunctions() throws SQLException {
        return "";
    }

    public String getTimeDateFunctions() throws SQLException {
        return "strtodate|strtodate?|strtotime|" +
                "strtotime?|strtotimestamp|strtotimestamp?";
    }

    public String getSearchStringEscape() throws SQLException {
        return "";
    }

    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return false;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    public boolean supportsConvert() throws SQLException {
        return false;
    }

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return false;
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    public boolean supportsOuterJoins() throws SQLException {
        throw new UnsupportedOperationException("not implemented supportsOuterJoins");
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        throw new UnsupportedOperationException("not implemented supportsFullOuterJoins");
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        throw new UnsupportedOperationException("not implemented supportsLimitedOuterJoins");
    }

    public String getSchemaTerm() throws SQLException {
        throw new UnsupportedOperationException("not implemented getSchemaTerm");
    }

    public String getProcedureTerm() throws SQLException {
        throw new UnsupportedOperationException("not implemented getProcedureTerm");
    }

    public String getCatalogTerm() throws SQLException {
        throw new UnsupportedOperationException("not implemented getCatalogTerm");
    }

    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    public String getCatalogSeparator() throws SQLException {
        return ",";
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    public boolean supportsUnion() throws SQLException {
        return true;
    }

    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxCharLiteralLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnsInIndex() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnsInSelect() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxColumnsInTable() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxConnections() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxCursorNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxIndexLength() throws SQLException {
        return 0x7FFFFFFF;
    }

    public int getMaxSchemaNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxProcedureNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 100000;
    }

    public int getMaxCatalogNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 1000000;
    }

    public int getMaxRowSize() throws SQLException {
        //TODO: Check the correct value to return
        return 1000000;
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    public int getMaxStatementLength() throws SQLException {
        //TODO: Check the correct value to return
        return 1000000;
    }

    public int getMaxStatements() throws SQLException {
        //TODO: Check the correct value to return
        return 1000;
    }

    public int getMaxTableNameLength() throws SQLException {
        //TODO: Check the correct value to return
        return 1000;
    }

    public int getMaxTablesInSelect() throws SQLException {
        //TODO: Check the correct value to return
        return 1000;
    }

    public int getMaxUserNameLength() throws SQLException {
        throw new UnsupportedOperationException("not implemented getMaxUserNameLength");
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        //TODO: Check the correct value to return
        return 0;
    }

    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getProcedures");
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getProcedureColumns");
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        throw new UnsupportedOperationException("not implemented getTables");
    }

    public ResultSet getSchemas() throws SQLException {
        throw new UnsupportedOperationException("not implemented getSchemas");
    }

    public ResultSet getCatalogs() throws SQLException {
        throw new UnsupportedOperationException("not implemented getCatalogs");
    }

    public ResultSet getTableTypes() throws SQLException {

        String[][] tables = new String[][]{{"TABLE"}, {"VIEW"}};
        Map<String, Integer> names = new LinkedHashMap<String, Integer>();
        names.put("table_type", 0);
        return new ArrayResultSet(tables, names);
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getColumns");
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getColumnPrivileges");
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getTablePrivileges");
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new UnsupportedOperationException("not implemented getBestRowIdentifier");
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException("not implemented getVersionColumns");
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException("not implemented getPrimaryKeys");
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException("not implemented getImportedKeys");
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new UnsupportedOperationException("not implemented getExportedKeys");
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new UnsupportedOperationException("not implemented getCrossReference");
    }

    public ResultSet getTypeInfo() throws SQLException {
        throw new UnsupportedOperationException("not implemented getTypeInfo");
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new UnsupportedOperationException("not implemented getIndexInfo");
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        //for the moment we only support the advance only
        return type == 0;
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throw new UnsupportedOperationException("not implemented getUDTs");
    }

    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("not implemented getConnection");
    }

    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getSuperTypes");
    }

    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getSuperTables");
    }

    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getAttributes");
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    public int getResultSetHoldability() throws SQLException {
        throw new UnsupportedOperationException("not implemented getResultSetHoldability");
    }

    public int getDatabaseMajorVersion() throws SQLException {
        throw new UnsupportedOperationException("not implemented getDatabaseMajorVersion");
    }

    public int getDatabaseMinorVersion() throws SQLException {
        throw new UnsupportedOperationException("not implemented getDatabaseMinorVersion");
    }

    public int getJDBCMajorVersion() throws SQLException {
        throw new UnsupportedOperationException("not implemented getJDBCMajorVersion");
    }

    public int getJDBCMinorVersion() throws SQLException {
        throw new UnsupportedOperationException("not implemented getJDBCMinorVersion");
    }

    public int getSQLStateType() throws SQLException {
        throw new UnsupportedOperationException("not implemented getSQLStateType");
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    public RowIdLifetime getRowIdLifetime() throws SQLException {
        throw new UnsupportedOperationException("not implemented getRowIdLifetime");
    }

    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getSchemas");
    }

    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    public ResultSet getClientInfoProperties() throws SQLException {
        throw new UnsupportedOperationException("not implemented getClientInfoProperties");
    }

    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getFunctions");
    }

    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getFunctionColumns");
    }

    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        throw new UnsupportedOperationException("not implemented getPseudoColumns");
    }

    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("not implemented isWrapperFor");
    }
}
