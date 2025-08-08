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

public class MedicalFacilityCategory {

    public static void main(String[] args) throws Exception {

        String tableName = "medical_facility_category";
        String tableNameOld = "medical_facility_category_old";

        SparkSession spark = SparkSession.builder()
                .appName(tableName)
                .master("local[*]")
                .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
                .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg.uri", "http://10.6.8.29:19120/api/v1")
                .config("spark.sql.catalog.iceberg.authentication.type", "NONE")
                .config("spark.sql.catalog.iceberg.ref", "main")
                .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
                .config("spark.sql.catalog.iceberg.s3.endpoint", "http://10.6.8.29:9000")
                .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
                .config("spark.sql.catalog.iceberg.s3.access-key-id", "pFt4lvYRBaKgUnEpB7Cr")
                .config("spark.sql.catalog.iceberg.s3.secret-access-key", "svE59joRKg9w4lMJbRCdyTLXd5u6QXxGU4OAraFF")
                .config("spark.sql.catalog.iceberg.s3.region", "us-east-1")
                .config("spark.hadoop.aws.region", "us-east-1")
                .getOrCreate();

        // Đọc Excel vào danh sách Java
        List<Row> data = new ArrayList<>();
        StructType schema = new StructType()
                .add("ma_cskb", DataTypes.StringType)
                .add("ten_cskb", DataTypes.StringType)
                .add("dia_chi", DataTypes.StringType)
                .add("level", DataTypes.StringType)            // từ medical_level_code
                .add("rank", DataTypes.StringType)             // từ medical_rank_code
                .add("ma_tinh", DataTypes.StringType)          // từ provinces_code
                .add("ma_tinh_int", DataTypes.StringType)      // từ provinces_id
                .add("created_at", DataTypes.StringType)
                .add("updated_at", DataTypes.StringType)
                .add("created_by", DataTypes.StringType)
                .add("updated_by", DataTypes.StringType);
        File xmlFile = new File("E:/app-msc/gmedical/29/medical_facility_category_202507311409.xml");
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
            values.add(getText(row, "ma_cskb"));               // 1
            values.add(getText(row, "ten_cskb"));              // 2
            values.add(getText(row, "dia_chi"));               // 3
            values.add(getText(row, "medical_level_code"));    // 4 → level
            values.add(getText(row, "medical_rank_code"));     // 5 → rank
            values.add(getText(row, "provinces_code"));        // 6 → ma_tinh
            values.add(getText(row, "provinces_id"));          // 7 → ma_tinh_int
            values.add(getText(row, "created_at"));            // 8
            values.add(getText(row, "updated_at"));            // 9
            values.add(getText(row, "created_by"));            // 10
            values.add(getText(row, "updated_by"));            // 11

            data.add(RowFactory.create(values.toArray()));
        }

        System.out.println("Tổng số dòng đọc được: " + data.size());

        Dataset<Row> df = spark.createDataFrame(data, schema);

        // Tạo database nếu chưa có
        spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.db_3179");
//        spark.sql("DROP TABLE IF EXISTS iceberg.db_3179."+tableNameOld);

        spark.sql("CREATE TABLE IF NOT EXISTS iceberg.db_3179.medical_facility_category (\n" +
                "    ma_cskb STRING,\n" +
                "    ten_cskb STRING,\n" +
                "    dia_chi STRING,\n" +
                "    level STRING,\n" +
                "    rank STRING,\n" +
                "    ma_tinh STRING,\n" +
                "    ma_tinh_int STRING,\n" +
                "    created_at STRING,\n" +
                "    updated_at STRING,\n" +
                "    created_by STRING,\n" +
                "    updated_by STRING\n" +
                ")\n" +
                "USING iceberg\n" +
                "TBLPROPERTIES (\n" +
                "    'write.metadata.delete-after-commit.enabled' = 'true',\n" +
                "    'write.metadata.previous-versions-max' = '1',\n" +
                "    'write.metadata.auto-merge.enabled' = 'false',\n" +
                "    'write.parquet.compression-codec' = 'uncompressed',\n" +
                "    'format-version' = '2',\n" +
                "    'write.format.default' = 'parquet'\n" +
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
