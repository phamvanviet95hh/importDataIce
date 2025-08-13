package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class FacilityDeseaseCode {
    public static void main(String[] args) throws Exception {

        String tableName = "facility_desease_code";
        String tableNameOld = "facility_desease_code_old";

        SparkSession spark = SparkSession.builder()
                .appName("ExcelToIcebergSpark")
                .master("local[*]")
                .config("spark.driver.host", "localhost") // <- fix lỗi hostname
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg.uri", "http://10.6.8.29:19120/api/v1")
                .config("spark.sql.catalog.iceberg.authentication.type", "NONE")
                .config("spark.sql.catalog.iceberg.ref", "main")
                .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
                .config("spark.sql.catalog.iceberg.s3.endpoint", "http://10.6.8.29:9000")
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
                .config("spark.sql.catalog.iceberg.s3.access-key-id", "NQbyMUVSv4sYrTEtsHB8")
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", "Wp4A9AroTCPEKKnipcpGvCMrRajXEZbnBDkap9y0")
                .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
                .config("spark.shuffle.push.enabled", "false")  // tắt tính năng gây lỗi!
                .getOrCreate();


        // Đọc Excel vào danh sách Java
        List<Row> data = new ArrayList<>();
        StructType schema = new StructType()
                .add("stt", DataTypes.StringType)
                .add("chapter_code", DataTypes.StringType)
                .add("chapter_name_en", DataTypes.StringType)
                .add("chapter_name_vn", DataTypes.StringType)
                .add("group_code", DataTypes.StringType)
                .add("group_name_en", DataTypes.StringType)
                .add("group_name_vn", DataTypes.StringType)
                .add("type_code", DataTypes.StringType)
                .add("type_name_en", DataTypes.StringType)
                .add("type_name_vn", DataTypes.StringType)
                .add("sick_code", DataTypes.StringType)
                .add("sick_code_v2", DataTypes.StringType)
                .add("sick_name_en", DataTypes.StringType)
                .add("sick_name_vn", DataTypes.StringType);
        File xmlFile = new File("E:\\app-msc\\gmedical\\29\\facility_desease_code_202508081129.xml");
        if (!xmlFile.exists()) {
            System.out.println("File không tồn tại!");
            return;
        }
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList list = doc.getElementsByTagName("DATA_RECORD");
        System.out.println("Tổng số dòng trong file XML: " + list.getLength());

        for (int i = 0; i < list.getLength(); i++) {
            Node row = list.item(i);
            List<String> values = new ArrayList<>();
            values.add(getText(row, "stt"));
            values.add(getText(row, "chapter_code"));
            values.add(getText(row, "chapter_name_en"));
            values.add(getText(row, "chapter_name_vn"));
            values.add(getText(row, "group_code"));
            values.add(getText(row, "group_name_en"));
            values.add(getText(row, "group_name_vn"));
            values.add(getText(row, "type_code"));
            values.add(getText(row, "type_name_en"));
            values.add(getText(row, "type_name_vn"));
            values.add(getText(row, "sick_code"));
            values.add(getText(row, "sick_code_v2"));
            values.add(getText(row, "sick_name_en"));
            values.add(getText(row, "sick_name_vn"));
            data.add(RowFactory.create(values.toArray()));
        }

        System.out.println("Tổng số dòng đọc được: " + data.size());

        Dataset<Row> df = spark.createDataFrame(data, schema);
        System.out.println("DataFrame Schema: " + data);

        // Tạo database nếu chưa có
        spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.db_3179");
        spark.sql("DROP TABLE IF EXISTS iceberg.db_3179."+tableNameOld);
        spark.sql("CREATE TABLE IF NOT EXISTS iceberg.db_3179."+tableName+" (\n" +
                "    stt STRING,\n" +
                "    chapter_code STRING,\n" +
                "    chapter_name_en STRING,\n" +
                "    chapter_name_vn STRING,\n" +
                "    group_code STRING,\n" +
                "    group_name_en STRING,\n" +
                "    group_name_vn STRING,\n" +
                "    type_code STRING,\n" +
                "    type_name_en STRING,\n" +
                "    type_name_vn STRING,\n" +
                "    sick_code STRING,\n" +
                "    sick_code_v2 STRING,\n" +
                "    sick_name_en STRING,\n" +
                "    sick_name_vn STRING\n" +
                ")\n" +
                "USING iceberg\n" +
                "TBLPROPERTIES (\n" +
                "  'format-version' = '2',\n" +
                "  'write.format.default' = 'parquet',\n" +
                "  'write.parquet.compression-codec' = 'zstd',\n" +
                "  'write.ordering' = 'soCccd, uuid, createdAt',\n" +
                "  'write.metadata.auto-merge.enabled' = 'false',\n" +
                "  'write.metadata.delete-after-commit.enabled' = 'true',\n" +
                "  'write.metadata.previous-versions-max' = '1'\n" +
                ")");


        // Ghi dữ liệu vào Iceberg table
        df.writeTo("iceberg.db_3179."+tableName).append();

        spark.stop();

    }

    public static String getText(Node node, String tagName) {
        try {
            Element element = (Element) node;
            NodeList tagList = element.getElementsByTagName(tagName);
            if (tagList.getLength() > 0 && tagList.item(0).getFirstChild() != null) {
                return tagList.item(0).getTextContent().trim();
            }
        } catch (Exception e) {
            // Có thể log lỗi nếu cần
        }
        return "";
    }
}
