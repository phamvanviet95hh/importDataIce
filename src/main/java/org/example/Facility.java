package org.example;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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


public class Facility {
    public static void main(String[] args) throws Exception {

        String tableName = "facility";
        String tableNameOld = "facility";

        SparkSession spark = SparkSession.builder()
                .appName("ExcelToIcebergSpark")
                .master("local[*]")
//                .config("spark.driver.host", "localhost") // <- fix lỗi hostname
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
                .config("spark.shuffle.push.enabled", "false")  // tắt tính năng gây lỗi!
                .getOrCreate();


        // Đọc Excel vào danh sách Java
        List<Row> data = new ArrayList<>();
        StructType schema = new StructType()
                .add("ma_cskb", DataTypes.StringType)
                .add("api_key", DataTypes.StringType)
                .add("co_so", DataTypes.StringType)
                .add("consumer_id", DataTypes.StringType)
                .add("created_at", DataTypes.StringType)
                .add("created_by", DataTypes.StringType)
                .add("dia_chi", DataTypes.StringType)
                .add("dien_thoai", DataTypes.StringType)
                .add("du_phong", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("hinh_thuc_to_chuc", DataTypes.StringType)
                .add("kinh_do", DataTypes.StringType)
                .add("loai_cskb", DataTypes.StringType)
                .add("ma_buu_chinh", DataTypes.StringType)
                .add("ma_huyen", DataTypes.StringType)
                .add("ma_phuong", DataTypes.StringType)
                .add("ma_so_thue", DataTypes.StringType)
                .add("ma_tinh", DataTypes.StringType)
                .add("ngay_cap", DataTypes.StringType)
                .add("nguoi_chiu_tn", DataTypes.StringType)
                .add("private_key", DataTypes.StringType)
                .add("public_key", DataTypes.StringType)
                .add("so_gphd", DataTypes.StringType)
                .add("status", DataTypes.StringType)
                .add("ten_cskb", DataTypes.StringType)
                .add("ten_huyen", DataTypes.StringType)
                .add("ten_phuong", DataTypes.StringType)
                .add("ten_tinh", DataTypes.StringType)
                .add("updated_at", DataTypes.StringType)
                .add("user_id", DataTypes.StringType)
                .add("user_name", DataTypes.StringType)
                .add("vi_do", DataTypes.StringType)
                .add("updated_by", DataTypes.StringType)
                .add("rank", DataTypes.StringType)
                .add("level", DataTypes.StringType)
                .add("is_dashboard", DataTypes.StringType)
                .add("is_ecom", DataTypes.StringType)
                .add("is_emr", DataTypes.StringType)
                .add("is_emr_gotrust", DataTypes.StringType)
                .add("is_his", DataTypes.StringType)
                .add("is_lis", DataTypes.StringType)
                .add("is_ris_pacs", DataTypes.StringType);
        File xmlFile = new File("E:\\app-msc\\gmedical\\29\\facility_202507311052.xml");
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
            values.add(Main.getText(row, "ma_cskb"));
            values.add(Main.getText(row, "api_key"));
            values.add(Main.getText(row, "co_so"));
            values.add(Main.getText(row, "consumer_id"));
            values.add(Main.getText(row, "created_at"));
            values.add(Main.getText(row, "created_by"));
            values.add(Main.getText(row, "dia_chi"));
            values.add(Main.getText(row, "dien_thoai"));
            values.add(Main.getText(row, "du_phong"));
            values.add(Main.getText(row, "email"));
            values.add(Main.getText(row, "hinh_thuc_to_chuc"));
            values.add(Main.getText(row, "kinh_do"));
            values.add(Main.getText(row, "loai_cskb"));
            values.add(Main.getText(row, "ma_buu_chinh"));
            values.add(Main.getText(row, "ma_huyen"));
            values.add(Main.getText(row, "ma_phuong"));
            values.add(Main.getText(row, "ma_so_thue"));
            values.add(Main.getText(row, "ma_tinh"));
            values.add(Main.getText(row, "ngay_cap"));
            values.add(Main.getText(row, "nguoi_chiu_tn"));
            values.add(Main.getText(row, "private_key"));
            values.add(Main.getText(row, "public_key"));
            values.add(Main.getText(row, "so_gphd"));
            values.add(Main.getText(row, "status"));
            values.add(Main.getText(row, "ten_cskb"));
            values.add(Main.getText(row, "ten_huyen"));
            values.add(Main.getText(row, "ten_phuong"));
            values.add(Main.getText(row, "ten_tinh"));
            values.add(Main.getText(row, "updated_at"));
            values.add(Main.getText(row, "user_id"));
            values.add(Main.getText(row, "user_name"));
            values.add(Main.getText(row, "vi_do"));
            values.add(Main.getText(row, "updated_by"));
            values.add(Main.getText(row, "rank"));
            values.add(Main.getText(row, "level"));
            values.add(Main.getText(row, "is_dashboard"));
            values.add(Main.getText(row, "is_ecom"));
            values.add(Main.getText(row, "is_emr"));
            values.add(Main.getText(row, "is_emr_gotrust"));
            values.add(Main.getText(row, "is_his"));
            values.add(Main.getText(row, "is_lis"));
            values.add(Main.getText(row, "is_ris_pacs"));
            data.add(RowFactory.create(values.toArray()));
        }

        System.out.println("Tổng số dòng đọc được: " + data.size());

        Dataset<Row> df = spark.createDataFrame(data, schema);
        System.out.println("DataFrame Schema: " + data);

        // Tạo database nếu chưa có
        spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.db_3179");
//        spark.sql("DROP TABLE IF EXISTS iceberg.db_3179."+tableNameOld);
        spark.sql("CREATE TABLE IF NOT EXISTS iceberg.db_3179."+tableName+" (\n" +
                "    ma_cskb STRING,\n" +
                "    api_key STRING,\n" +
                "    co_so STRING,\n" +
                "    consumer_id STRING,\n" +
                "    created_at STRING,\n" +
                "    created_by STRING,\n" +
                "    dia_chi STRING,\n" +
                "    dien_thoai STRING,\n" +
                "    du_phong STRING,\n" +
                "    email STRING,\n" +
                "    hinh_thuc_to_chuc STRING,\n" +
                "    kinh_do STRING,\n" +
                "    loai_cskb STRING,\n" +
                "    ma_buu_chinh STRING,\n" +
                "    ma_huyen STRING,\n" +
                "    ma_phuong STRING,\n" +
                "    ma_so_thue STRING,\n" +
                "    ma_tinh STRING,\n" +
                "    ngay_cap STRING,\n" +
                "    nguoi_chiu_tn STRING,\n" +
                "    private_key STRING,\n" +
                "    public_key STRING,\n" +
                "    so_gphd STRING,\n" +
                "    status STRING,\n" +
                "    ten_cskb STRING,\n" +
                "    ten_huyen STRING,\n" +
                "    ten_phuong STRING,\n" +
                "    ten_tinh STRING,\n" +
                "    updated_at STRING,\n" +
                "    user_id STRING,\n" +
                "    user_name STRING,\n" +
                "    vi_do STRING,\n" +
                "    updated_by STRING,\n" +
                "    rank STRING,\n" +
                "    level STRING,\n" +
                "    is_dashboard STRING,\n" +
                "    is_ecom STRING,\n" +
                "    is_emr STRING,\n" +
                "    is_emr_gotrust STRING,\n" +
                "    is_his STRING,\n" +
                "    is_lis STRING,\n" +
                "    is_ris_pacs STRING\n" +
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
