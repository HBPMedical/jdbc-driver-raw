package raw.jdbc;

import raw.jdbc.rawclient.RawRestClient;
import raw.jdbc.rawclient.requests.SourceNameResponse;

import java.util.*;

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import raw.jdbc.rawclient.requests.ViewResponse;

/**
 * The raw database metada
 * TODO: Make less hardcoded values (creating services to get the info dynamically from the server?)
 */
public class RawDatabaseMetaData implements DatabaseMetaData {

    public static class SchemaInfoColumn {
        public String name;
        public String tipe;
    }

    public static class Table {
        public String name;
        public String catalog;
        public String type;
        public String schema;
        public SchemaInfoColumn[] columns;
    }

    private String url;
    private String user;
    private RawRestClient client;
    private Connection connection;
    private ArrayList<Table> tables;

    static Logger logger = Logger.getLogger(RawDatabaseMetaData.class.getName());

    RawDatabaseMetaData(String url, String user, RawRestClient client, Connection connection) throws SQLException {
        this.url = url;
        this.user = user;
        this.client = client;
        this.connection = connection;
        try {
            tables = new ArrayList<Table>();
            // gets the sources
            SourceNameResponse[] sources = this.client.getSources();
            for (SourceNameResponse s : sources) {
                logger.fine("Metadata adding source: " + s.name);
                Table source = new Table();
                source.name = s.name;
                source.schema = "sources";
                source.catalog = user;
                source.type = (String) s.location.get("type");
                source.columns = typeToColumns(s.tipe);
                tables.add(source);
            }
            // gets the views (queries?)
            ViewResponse[] views = this.client.getViews();
            for (ViewResponse v : views) {
                logger.fine("Metadata adding view: " + v.name);
                Table view = new Table();
                view.name = v.name;
                view.schema = "views";
                view.catalog = user;
                view.type = "view";
                view.columns = typeToColumns(v.tipe);
                tables.add(view);
            }
        } catch (IOException e) {
            throw new SQLException("could not get schema info " + e.getMessage());
        }
    }

    private static SchemaInfoColumn[] typeToColumns(LinkedHashMap<String, Object> obj) throws SQLException {
        String outerType = (String) obj.get("type");
        if (outerType.equals("collection")) {
            LinkedHashMap<String, Object> innerObj = (LinkedHashMap<String, Object>) obj.get("innerType");
            String innerType = (String) innerObj.get("type");
            if (innerType.equals("record")) {
                logger.fine("transforming: " + innerType);
                ArrayList<SchemaInfoColumn> columns = columnsFromRecord(innerObj);
                return columns.toArray(new SchemaInfoColumn[]{});
            } else {
                SchemaInfoColumn info = new SchemaInfoColumn();
                info.name = "element";
                info.tipe = printType(innerObj);
                return new SchemaInfoColumn[]{info};
            }
        } else {
            //TODO: handle non collection elements
            throw new SQLException("Unsupported type for schemas" + outerType);
        }
    }

    static ArrayList<SchemaInfoColumn> columnsFromRecord(LinkedHashMap<String, Object> record) throws SQLException {
        ArrayList<LinkedHashMap<String, Object>> atts = (ArrayList<LinkedHashMap<String, Object>>) record.get("atts");
        ArrayList<SchemaInfoColumn> columns = new ArrayList<SchemaInfoColumn>();
        for (LinkedHashMap<String, Object> attr : atts) {
            SchemaInfoColumn info = new SchemaInfoColumn();
            info.name = (String) attr.get("idn");
            info.tipe = printType((LinkedHashMap<String, Object>) attr.get("tipe"));
            columns.add(info);
        }
        return columns;
    }

    static String printType(LinkedHashMap<String, Object> obj) throws SQLException {
        String name = (String) obj.get("type");

        if (name.equals("int") ||
                name.equals("long") ||
                name.equals("string") ||
                name.equals("double") ||
                name.equals("float") ||
                name.equals("date") ||
                name.equals("date") ||
                name.equals("datetime") ||
                name.equals("boolean")) {
            return name;
        } else if (name.equals("collection")) {
            LinkedHashMap<String, Object> innerType = (LinkedHashMap<String, Object>) obj.get("innerType");
            return "collection(" + printType(innerType) + ")";
        } else if (name.equals("record")) {
            ArrayList<LinkedHashMap<String, Object>> atts = (ArrayList<LinkedHashMap<String, Object>>) obj.get("atts");
            ArrayList<String> attributes = new ArrayList<String>();
            for (LinkedHashMap<String, Object> attr : atts) {
                String attName = (String) attr.get("idn");
                String attType = printType((LinkedHashMap<String, Object>) attr.get("tipe"));
                attributes.add(attName + ":" + attType);
            }
            return "collection(" + StringUtils.join(attributes, ',') + ")";
        } else if (name.equals("option")) {
            LinkedHashMap<String, Object> innerType = (LinkedHashMap<String, Object>) obj.get("t");
            return "option(" + printType(innerType) + ")";
        } else {
            throw new SQLException("printType Unknown type " + name);
        }
    }

    static String typeToName(int type) {
        switch (type) {
            case Types.INTEGER:
                return "int";
            case Types.BIGINT:
                return "long";
            case Types.VARCHAR:
                return "string";
            case Types.DOUBLE:
                return "double";
            case Types.STRUCT:
                return "record";
            case Types.ARRAY:
                return "collection";
            case Types.BOOLEAN:
                return "boolean";
            default:
                return "unknow type";
        }
    }

    static int nameToType(String name) throws SQLException {
        if (name.equals("int")) {
            return Types.INTEGER;
        } else if (name.equals("long")) {
            return Types.BIGINT;
        } else if (name.equals("string")) {
            return Types.VARCHAR;
        } else if (name.equals("double")) {
            return Types.DOUBLE;
        } else if (name.equals("float")) {
            return Types.FLOAT;
        } else if (name.equals("date")) {
            return Types.DATE;
        } else if (name.equals("date")) {
            return Types.DATE;
        } else if (name.equals("datetime")) {
            return Types.TIME;
        } else if (name.equals("boolean")) {
            return Types.BOOLEAN;
        } else if (name.startsWith("collection")) {
            return Types.ARRAY;
        } else if (name.startsWith("record")) {
            return Types.STRUCT;
            //TODO: check what we should inform with optionTypes
        } else if (name.startsWith("option")) {
            // removes "option(" and the last ")"
            String newName = name.substring(7, name.length() - 1);
            return nameToType(newName);
        } else {
            throw new SQLException("nameToType Unknown type " + name);
        }
    }

    static int objToType(Object obj) throws SQLException {
        if (obj == null) {
            //return Types.NULL;
            return Types.VARCHAR;
        } else if (obj.getClass() == Integer.class) {
            return Types.INTEGER;
        } else if (obj.getClass() == Long.class) {
            return Types.BIGINT;
        } else if (obj.getClass() == String.class) {
            return Types.VARCHAR;
        } else if (obj.getClass() == Double.class) {
            return Types.DOUBLE;
        } else if (obj.getClass() == Boolean.class) {
            return Types.BOOLEAN;
        } else if (obj.getClass() == LinkedHashMap.class) {
            return Types.STRUCT;
        } else if (obj.getClass() == ArrayList.class) {
            return Types.ARRAY;
        } else if (obj.getClass().isArray()) {
            return Types.ARRAY;
        } else {
            throw new SQLException("Unsupported type " + obj.getClass());
        }
    }

    private static boolean isNullable(String type) {
        return type.startsWith("option");
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return false;
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return true;
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
        return "select,distinct,from,where,group,by,having,in,union,order,desc,asc," +
                "if,then,else,parse,parse?,into,not,and,or,flatten,like," +
                "as,as?,all,cast,partition,on,error,fail,skip,isnull,isnone,collection";
    }

    public String getNumericFunctions() throws SQLException {
        return "avg,count,max,min,min?,max?,sum";
    }

    public String getStringFunctions() throws SQLException {
        return "trim,startswith,strempty,strtodate,strtodate?," +
                "strtotime,strtotime?,strtotimestamp,strtotimestamp?";
    }

    public String getSystemFunctions() throws SQLException {
        return "";
    }

    public String getTimeDateFunctions() throws SQLException {
        return "strtodate,strtodate?,strtotime," +
                "strtotime?,strtotimestamp,strtotimestamp?";
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
        //This option was creating problems with squirrel sql,
        // check where in the implementation we are failing
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
        return true;
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    public String getSchemaTerm() throws SQLException {
        return "schema";
    }

    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    public String getCatalogTerm() throws SQLException {
        return "catalog";
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
        //TODO: Check the correct value to return
        return 1000000;
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

        String[] columns = new String[]{
                "PROCEDURE_CAT", // String = > procedure catalog(may be null)
                "PROCEDURE_SCHEM", // String =>procedure schema (may be null)
                "PROCEDURE_NAME", // String =>procedure name
                "reserved", // for future use
                "reserved", // for future use
                "reserved", // for future use
                "REMARKS", // String =>explanatory comment on the procedure
                "PROCEDURE_TYPE", // short =>kind of procedure:
                "procedureResultUnknown", // - Cannot determine if a return value will be returned
                "procedureNoResult", // - Does not return a return value
                "procedureReturnsResult", // - Returns a return value
                "SPECIFIC_NAME", // String =>The name which uniquely identifies this procedure within its schema.
        };
        int[] types = new int[]{
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.INTEGER,
                Types.VARCHAR
        };
        return new ArrayResultSet(new Object[][]{}, columns, types);
    }

    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getProcedureColumns");
    }

    private Table[] findTable(String catalogPattern, String schemaPattern, String tableNamePattern, String[] types) {
        ArrayList<Table> out = new ArrayList<Table>();
        for (Table info : tables) {
            if (!stringMatch(info.catalog, catalogPattern) ||
                    !stringMatch(info.schema, schemaPattern) ||
                    !stringMatch(info.name, tableNamePattern)) {
                continue;
            }

            // TODO: check the other types, like views etc.
            if (types == null) {
                out.add(info);
            } else if (Arrays.asList(types).contains(info.type)) {
                out.add(info);
            }
        }
        return out.toArray(new Table[]{});
    }

    private static boolean stringMatch(String value, String pattern) {
        if (pattern == null) {
            return true;
        } else if (value == null) {
            return false;
        } else {
            String regex = pattern.replaceAll("%", ".*");
            return Pattern.matches(regex, value);
        }
    }

    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        //TODO: Make the search work, check the difference for us between schema and table
        String[] columnNames = new String[]{
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"
        };

        Table[] matches = findTable(catalog, schemaPattern, tableNamePattern, types);
        logger.fine("matches length: " + matches.length + " schema length: " + tables.size());
        String[][] data = new String[matches.length][];
        for (int i = 0; i < matches.length; i++) {
            logger.fine("Adding: " + matches[i].name + ", type:" + matches[i].type);
            data[i] = new String[]{
                    matches[i].catalog, matches[i].schema, matches[i].name, matches[i].type, "user table",
                    null, null, null, null, null};

        }

        int[] mdTypes = new int[]{Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
        return new ArrayResultSet(data, columnNames, mdTypes);
    }

    public ResultSet getSchemas() throws SQLException {

        String[][] rsSchemas = new String[][]{
                {"sources", user},
                {"views", user}
        };
        String[] columnNames = new String[]{"TABLE_SCHEM", "TABLE_CATALOG"};
        return new ArrayResultSet(rsSchemas, columnNames);
    }

    public ResultSet getCatalogs() throws SQLException {
        String[][] data = new String[][]{{user}};
        return new ArrayResultSet(data, new String[]{"TABLE_CAT"});
    }

    //TODO: define better the types of columns
    public ResultSet getTableTypes() throws SQLException {
        // we are getting all unique types, we could just use a fixed list:
        // String data[][] = new String[][] {{"local_file"}, {"local_files"}, {"local_directory"}, {"http_file"}, {"s3_file"}, {"s3_directory"}, {"hdfs_file"}, {"s3_files"}, {"rdbms_table"}}
        Set<String> types = new HashSet<String>();
        for (Table t : tables) {
            types.add(t.type);
        }
        String[][] data = new String[types.size()][1];
        int n = 0;
        for (String t : types) {
            data[n++][0] = t;
        }
        String[] fields = new String[]{"TABLE_TYPE "};
        return new ArrayResultSet(data, fields);
    }

    static private int type2ColumnSize(String type) {
        if (type.equals("string")) {
            return 20;
        } else if (type.startsWith("collection") || type.startsWith("record")) {
            return 100;
        } else {
            return 5;
        }
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String[] fields = new String[]{
                "TABLE_CAT", //(String)  table catalog (may be null)
                "TABLE_SCHEM", //(String)  table schema (may be null)
                "TABLE_NAME", //(String)  table name
                "COLUMN_NAME", //(String)  column name
                "DATA_TYPE", //(int)  SQL type from java.sql.Types
                "TYPE_NAME", //(String)  Data source dependent type name, for a UDT the type name is fully qualified
                "COLUMN_SIZE", //(int)  column size.
                "BUFFER_LENGTH", // is not used.
                "DECIMAL_DIGITS", //(int)  the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
                "NUM_PREC_RADIX", //(int)  Radix (typically either 10 or 2)
                "NULLABLE", //(int)  is NULL allowed.
                //columnNoNulls - might not allow NULL values
                //columnNullable - definitely allows NULL values
                //columnNullableUnknown - nullability unknown
                "REMARKS", //(String)  comment describing column (may be null)
                "COLUMN_DEF", //(String)  default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
                "SQL_DATA_TYPE", //(int)  unused
                "SQL_DATETIME_SUB", //(int)  unused
                "CHAR_OCTET_LENGTH", //(int)  for char types the maximum number of bytes in the column
                "ORDINAL_POSITION", //(int)  index of column in table (starting at 1)
                "IS_NULLABLE", //(String)  ISO rules are used to determine the nullability for a column.
                //YES-- - if the column can include NULLs
                //NO-- - if the column cannot include NULLs
                //empty string --- if the nullability for the column is unknown
                "SCOPE_CATALOG", //(String)  catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
                "SCOPE_SCHEMA", //(String)  schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
                "SCOPE_TABLE", //(String)  table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
                "SOURCE_DATA_TYPE", //(short)  source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
                "IS_AUTOINCREMENT", //(String)  Indicates whether this column is auto incremented
                //YES-- - if the column is auto incremented
                //NO-- - if the column is not auto incremented
                //empty string --- if it cannot be determined whether the column is auto incremented
                "IS_GENERATEDCOLUMN", //(String)  Indicates whether this is a generated column
                //YES-- - if this a generated column
                //NO-- - if this not a generated column
                //empty string --- if it cannot be determined whether this is a generated column
        };

        Table[] matches = findTable(catalog, schemaPattern, tableNamePattern, null);

        ArrayList<Object[]> data = new ArrayList<Object[]>();
        for (Table info : matches) {

            for (int i = 0; i < info.columns.length; i++) {
                SchemaInfoColumn col = info.columns[i];
                if (stringMatch(col.name, columnNamePattern)) {
                    Object[] obj = new Object[]{
                            info.catalog,
                            info.schema,
                            info.name,
                            col.name,
                            nameToType(col.tipe),
                            col.tipe,
                            type2ColumnSize(col.tipe), // to review
                            0,      // to review
                            3,      // to review DECIMAL_DIGITS
                            10,
                            isNullable(col.tipe) ? columnNullable : columnNoNulls,
                            "info got from rest service",
                            null,
                            0,
                            0,
                            1000000,
                            i + 1,
                            isNullable(col.tipe) ? "YES" : "NO",
                            null,
                            null,
                            null,
                            null,
                            "NO",
                            "NO"
                    };
                    data.add(obj);
                }
            }
        }

        int[] objTypes = new int[]{
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.INTEGER,
                Types.VARCHAR,
                Types.INTEGER,
                Types.INTEGER,
                Types.INTEGER,
                Types.INTEGER,
                Types.INTEGER,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.INTEGER,
                Types.INTEGER,
                Types.INTEGER,
                Types.INTEGER,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR,
                Types.VARCHAR
        };
        return new ArrayResultSet(data.toArray(new Object[][]{}), fields, objTypes);
    }

    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        //TODO: implement this
        Object[][] data = new Object[][]{};
        String[] fields = new String[]{
                "TABLE_CAT", //(String)  table catalog (may be null)
                "TABLE_SCHEM", //(String)  table schema (may be null)
                "TABLE_NAME", //(String)  table name
                "COLUMN_NAME", //(String)  column name
                "GRANTOR", //(String)  grantor of access (may be null)
                "GRANTEE", //(String)  grantee of access
                "PRIVILEGE", //(String)  name of access (SELECT, INSERT, UPDATE, REFRENCES, ...)
                "IS_GRANTABLE" //(String)  "YES" if grantee is permitted to grant to others; "NO" if not; null if unknown
        };

        throw new SQLFeatureNotSupportedException("not implemented getColumnPrivileges");
        //return new ArrayResultSet(data, fields);
    }

    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getTablePrivileges");
    }

    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getBestRowIdentifier");
    }

    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getVersionColumns");
    }

    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getPrimaryKeys");
    }

    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getImportedKeys");
    }

    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getExportedKeys");
    }

    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getCrossReference");
    }

    public ResultSet getTypeInfo() throws SQLException {
        String[] fields = new String[]{
                "TYPE_NAME", //(String)  Type name
                "DATA_TYPE", //(int)  SQL data type from java.sql.Types
                "PRECISION", //(int)  maximum precision
                "LITERAL_PREFIX", //(String)  prefix used to quote a literal (may be null)
                "LITERAL_SUFFIX", //(String)  suffix used to quote a literal (may be null)
                "CREATE_PARAMS", //(String)  parameters used in creating the type (may be null)
                "NULLABLE", //(short)  can you use NULL for this type.   typeNoNulls - does not allow NULL values
                // typeNullable - allows NULL values
                // typeNullableUnknown - nullability unknown
                "CASE_SENSITIVE", //(boolean)  is it case sensitive.
                "SEARCHABLE", //(short)  can you use "WHERE" based on this type:
                // typePredNone - No support
                //typePredChar - Only supported with WHERE .. LIKE
                // typePredBasic - Supported except for WHERE .. LIKE
                //typeSearchable - Supported for all WHERE ..
                "UNSIGNED_ATTRIBUTE", //(boolean)  is it unsigned.
                "FIXED_PREC_SCALE", //(boolean)  can it be a money value.
                "AUTO_INCREMENT", //(boolean)  can it be used for an auto-increment value.
                "LOCAL_TYPE_NAME", //(String)  localized version of type name (may be null)
                "MINIMUM_SCALE", //(short)  minimum scale supported
                "MAXIMUM_SCALE", //(short)  maximum scale supported
                "SQL_DATA_TYPE", //(int)  unused
                "SQL_DATETIME_SUB", //(int)  unused
                "NUM_PREC_RADIX", //(int)  usually 2 or 10
        };
        Object[][] data = new Object[][]{
                {"int", Types.INTEGER, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "int", 0, 0, Types.INTEGER, 0, 10},
                {"string", Types.VARCHAR, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "string", 0, 0, Types.VARCHAR, 0, 10},
                {"long", Types.BIGINT, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "long", 0, 0, 0, Types.BIGINT, 10},
                {"double", Types.DOUBLE, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "double", 0, 0, Types.DOUBLE, 0, 10},
                {"collection", Types.ARRAY, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "collection", 0, 0, Types.ARRAY, 0, 10},
                {"record", Types.STRUCT, 0Xffffffff, null, null, null, typeNullable, true, typeSearchable, false, false, false, "record", 0, 0, Types.STRUCT, 0, 10},
        };
        return new ArrayResultSet(data, fields);
    }

    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        throw new SQLFeatureNotSupportedException("not implemented getIndexInfo");
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        //for the moment we only support the advance only
        return type == ResultSet.TYPE_FORWARD_ONLY;
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
        //TODO: check if we have a way of getting the user defined types
        String[] fields = new String[]{
                "TYPE_CAT", //(String)  the type's catalog (may be null)
                "TYPE_SCHEM", //(String)  type's schema (may be null)
                "TYPE_NAME", //(String)  type name
                "CLASS_NAME", //(String)  Java class name
                "DATA_TYPE", //(int)  type value defined in java.sql.Types. One of JAVA_OBJECT, STRUCT, or DISTINCT
                "REMARKS", //(String)  explanatory comment on the type
                "BASE_TYPE" //(short)  type code of the source type of a DISTINCT type or the type that implements the user-generated reference type of the SELF_REFERENCING_COLUMN of a structured type as defined in java.sql.Types (null if DATA_TYPE is not DISTINCT or not STRUCT with REFERENCE_GENERATION = USER_DEFINED)
        };

        // for now we are returning an empty recordset
        Object[][] data = new Object[][]{};
        throw new UnsupportedOperationException("not implemented getUDTs");
        //return new ArrayResultSet(data, fields);
    }

    public Connection getConnection() throws SQLException {
        return this.connection;
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
        return 0;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return 1;
    }

    public int getJDBCMajorVersion() throws SQLException {
        //JDBC 4.2, is specified by a maintenance release 2 of JSR 221 and is included in Java SE 8.
        //TODO: Confirm if this is correct
        return 4;
    }

    public int getJDBCMinorVersion() throws SQLException {
        //TODO: Confirm if this is correct
        return 2;
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
        //TODO: implement search
        return getSchemas();
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
