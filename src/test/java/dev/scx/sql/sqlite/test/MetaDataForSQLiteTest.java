package dev.scx.sql.sqlite.test;

import dev.scx.sql.metadata.CatalogMetadata;
import dev.scx.sql.metadata.SchemaMetadata;
import dev.scx.sql.sqlite.SQLiteDialect;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.List;

import static dev.scx.sql.metadata.DatabaseMetadataReader.*;
import static dev.scx.sql.sqlite.test.SQLClientForSQLiteTest.dataSource;

public class MetaDataForSQLiteTest {

    public static void main(String[] args) throws SQLException {
        test1();
    }

    @Test
    public static void test1() throws SQLException {
        System.out.println("按需内省 !!!");
        var dialect = new SQLiteDialect();
        try (var con = dataSource.getConnection()) {
            var metaData = con.getMetaData();

            var catalogMetadataList = readCatalogMetadataList(metaData);
            // 如果没有我们使用虚拟层级
            if (catalogMetadataList.isEmpty()) {
                catalogMetadataList = List.of(new CatalogMetadata(null));
            }

            for (var catalogMetadata : catalogMetadataList) {
                System.out.println(catalogMetadata.TABLE_CAT() + " : ");
                System.out.println("{");

                List<SchemaMetadata> schemaMetadataList = List.of();
                try {
                    schemaMetadataList = readSchemaMetadataList(metaData, catalogMetadata.TABLE_CAT(), null);
                } catch (SQLException _) {
                    // 忽略错误
                }

                // 如果没有我们使用虚拟层级
                if (schemaMetadataList.isEmpty()) {
                    schemaMetadataList = List.of(new SchemaMetadata(null, catalogMetadata.TABLE_CAT()));
                }

                for (var schemaMetadata : schemaMetadataList) {
                    System.out.println("    " + schemaMetadata.TABLE_SCHEM() + " : ");
                    System.out.println("    {");

                    var tableMetadataList = readTableMetadataList(metaData, schemaMetadata.TABLE_CATALOG(), schemaMetadata.TABLE_SCHEM(), null);

                    for (var tableMetadata : tableMetadataList) {
                        var table = loadTable(metaData, tableMetadata.TABLE_CAT(), tableMetadata.TABLE_SCHEM(), tableMetadata.TABLE_NAME(), dialect);
                        System.out.println("        建表 DDL");
                        var createTableDDLs = dialect.getCreateTableDDLs(table);
                        for (var createTableDDL : createTableDDLs) {
                            // 追加 缩进
                            var lines = createTableDDL.split("\n");
                            for (String line : lines) {
                                System.out.println("        " + line);
                            }
                        }
                        System.out.println("        改表 DDL");
                        for (var column : table.columns()) {
                            var addColumnDDLs = dialect.getAddColumnDDLs(table, column);
                            for (var addColumnDDL : addColumnDDLs) {
                                System.out.println("        " + addColumnDDL);
                            }
                        }
                        // 默认我们只保守 add 没有的索引.
                        for (var index : table.indexes()) {
                            var addIndexDDLs = dialect.getAddIndexDDLs(table, index);
                            for (var addIndexDDL : addIndexDDLs) {
                                System.out.println("        " + addIndexDDL);
                            }
                        }
                    }

                    System.out.println("    }");
                }
                System.out.println("}");
            }
        }

    }

}
