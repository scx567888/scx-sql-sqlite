package dev.scx.sql.sqlite;

import dev.scx.sql.JDBCConnectionInfo;
import dev.scx.sql.dialect.Dialect;
import dev.scx.sql.schema.*;
import org.sqlite.JDBC;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;

/// SQLiteDialect
///
/// @author scx567888
/// @version 0.0.1
/// @see <a href="https://www.sqlite.org/lang_createtable.html">https://www.sqlite.org/lang_createtable.html</a>
public final class SQLiteDialect implements Dialect {

    private static final JDBC DRIVER = initDRIVER();

    public SQLiteDialect() {
    }

    private static JDBC initDRIVER() {
        try {
            return new JDBC();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean canHandle(String url) {
        return DRIVER.acceptsURL(url);
    }

    @Override
    public boolean canHandle(DataSource dataSource) {
        try {
            return dataSource instanceof SQLiteDataSource || dataSource.isWrapperFor(SQLiteDataSource.class);
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public DataSource createDataSource(JDBCConnectionInfo connectionInfo) {
        var sqLiteDataSource = new SQLiteDataSource();
        sqLiteDataSource.setUrl(connectionInfo.url());
        return sqLiteDataSource;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public DataTypeKind dialectTypeNameToDataTypeKind(String dialectTypeName) {
        return SQLiteDialectHelper.dialectTypeNameToDataTypeKind(dialectTypeName);
    }

    @Override
    public String dataTypeKindToDialectTypeName(DataTypeKind dataTypeKind) {
        return SQLiteDialectHelper.dataTypeKindToDialectTypeName(dataTypeKind);
    }

    @Override
    public List<String> getCreateTableDDLs(Table table) {
        var ddls = new ArrayList<String>();

        var sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        sb.append(getQualifiedTableName(table)).append("\n");
        sb.append("(").append("\n");

        var joiner = new StringJoiner(",\n    ", "    ", "");

        // 先找出主键列
        var primaryKeyColumns = getPrimaryKeyColumns(table);

        // 追加列定义
        for (var column : table.columns()) {
            joiner.add(getColumnDefinition(column, primaryKeyColumns));
        }

        // 如果没有走列级 PRIMARY KEY AUTOINCREMENT，
        // 且存在主键，则补一个表级 PRIMARY KEY
        var inlinePrimaryKeyColumn = getInlinePrimaryKeyAutoIncrementColumn(table, primaryKeyColumns);
        if (inlinePrimaryKeyColumn == null && !primaryKeyColumns.isEmpty()) {
            joiner.add("PRIMARY KEY (" + joinQuotedColumns(primaryKeyColumns) + ")");
        }

        sb.append(joiner);
        sb.append("\n").append(");");

        ddls.add(sb.toString());

        // SQLite 的唯一索引 / 普通索引更适合单独 CREATE INDEX
        for (var index : table.indexes()) {
            ddls.addAll(getAddIndexDDLs(table, index));
        }

        return ddls;
    }

    @Override
    public List<String> getAddColumnDDLs(Table table, Column column) {
        var ddl = "ALTER TABLE " + getQualifiedTableName(table) + " ADD COLUMN " + getColumnDefinition(column, List.of()) + ";";
        return List.of(ddl);
    }

    @Override
    public List<String> getDropColumnDDLs(Table table, Column column) {
        var ddl = "ALTER TABLE " + getQualifiedTableName(table) + " DROP COLUMN " + quoteIdentifier(column.name()) + ";";
        return List.of(ddl);
    }

    @Override
    public List<String> getAddIndexDDLs(Table table, Index index) {
        var sb = new StringBuilder();

        if (index.unique()) {
            sb.append("CREATE UNIQUE INDEX ");
        } else {
            sb.append("CREATE INDEX ");
        }

        sb.append(quoteIdentifier(index.name()))
            .append(" ON ")
            .append(getQualifiedTableName(table))
            .append(" (")
            .append(quoteIdentifier(index.columnName()))
            .append(");");

        return List.of(sb.toString());
    }

    @Override
    public List<String> getDropIndexDDLs(Table table, Index index) {
        var ddl = "DROP INDEX " + quoteIdentifier(index.name()) + ";";
        return List.of(ddl);
    }

    /// 获取数据类型定义
    private String getDataTypeDefinition(DataType dataType) {
        // SQLite 的类型通常不需要长度
        return dataTypeKindToDialectTypeName(dataType.kind());
    }

    /// 获取列约束
    private List<String> getColumnConstraints(Column column) {
        var list = new ArrayList<String>();

        if (column.notNull()) {
            list.add("NOT NULL");
        }

        if (column.defaultValue() != null && !column.defaultValue().isBlank()) {
            list.add("DEFAULT " + column.defaultValue());
        }

        return list;
    }

    /// 获取列定义
    private String getColumnDefinition(Column column, List<String> primaryKeyColumns) {
        var parts = new ArrayList<String>();

        parts.add(quoteIdentifier(column.name()));

        // 如果该列是唯一的主键列，且声明了自增，则走 SQLite 特殊语法:
        // INTEGER PRIMARY KEY AUTOINCREMENT
        if (isInlinePrimaryKeyAutoIncrementColumn(column, primaryKeyColumns)) {
            parts.add("INTEGER");
            parts.add("PRIMARY KEY");
            parts.add("AUTOINCREMENT");
            return String.join(" ", parts);
        }

        parts.add(getDataTypeDefinition(column.dataType()));
        parts.addAll(getColumnConstraints(column));

        return String.join(" ", parts);
    }

    private String getQualifiedTableName(Table table) {
        var schemaName = table.schema();
        var tableName = table.name();

        if (schemaName != null && !schemaName.isEmpty()) {
            return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName);
        }

        return quoteIdentifier(tableName);
    }

    private List<String> getPrimaryKeyColumns(Table table) {
        var list = new ArrayList<String>();
        var seen = new HashSet<String>();

        for (var key : table.keys()) {
            if (key.primary() && seen.add(key.columnName())) {
                list.add(key.columnName());
            }
        }

        return list;
    }

    private String getInlinePrimaryKeyAutoIncrementColumn(Table table, List<String> primaryKeyColumns) {
        if (primaryKeyColumns.size() != 1) {
            return null;
        }

        var pkColumnName = primaryKeyColumns.get(0);
        for (var column : table.columns()) {
            if (!column.name().equals(pkColumnName)) {
                continue;
            }
            if (!column.autoIncrement()) {
                return null;
            }
            return pkColumnName;
        }

        return null;
    }

    private boolean isInlinePrimaryKeyAutoIncrementColumn(Column column, List<String> primaryKeyColumns) {
        return primaryKeyColumns.size() == 1
            && primaryKeyColumns.get(0).equals(column.name())
            && column.autoIncrement();
    }

    private String joinQuotedColumns(List<String> columns) {
        var joiner = new StringJoiner(", ");
        for (var column : columns) {
            joiner.add(quoteIdentifier(column));
        }
        return joiner.toString();
    }

}
