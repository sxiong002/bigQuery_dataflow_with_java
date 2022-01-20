import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.DataflowRunner;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.*;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

//import org.apache.beam.examples.snippets.transforms.io.gcp.bigquery.BigQueryMyData.MyData;
 import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;

import com.google.api.services.bigquery.model.TableFieldSchema;
import java.util.Arrays;



public class bigQuery_dataflow {


    public static void main(String[] args) {

//        dataflow pipeline options for pipeline setting
        DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowOptions.setProject("york-cdf-start");
        dataflowOptions.setRegion("us-central1");
        dataflowOptions.setJobName("soa-xiong-java-capstone");
        dataflowOptions.setTempLocation("gs://york-project-bucket/soa-xiong/java/tmp");
        dataflowOptions.setStagingLocation("gs://york-project-bucket/soa-xiong/java/staging");
        //dataflowOptions.setRunner(DataflowRunner.class);

//        pipeline Creation and adding options parameter
        Pipeline p = Pipeline.create(dataflowOptions);


//        Schema creation with TableSchema import
        TableSchema products_schema = new TableSchema().setFields(Arrays.asList(new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                                      new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                                      new TableFieldSchema().setName("total_no_of_product_views").setType("INTEGER").setMode("REQUIRED")
                               ));
        TableSchema total_sales_schema = new TableSchema().setFields(Arrays.asList(new TableFieldSchema().setName("cust_tier_code").setType("STRING").setMode("REQUIRED"),
                                         new TableFieldSchema().setName("sku").setType("INTEGER").setMode("REQUIRED"),
                                         new TableFieldSchema().setName("total_sales_amount").setType("FLOAT").setMode("REQUIRED")
        ));

//        Reading from BigQuery
        PCollection<TableRow> products_view_rows =
                p.apply("Read from BigQuery query",
                    BigQueryIO.readTableRows()
                            .fromQuery("SELECT cast(c.CUST_TIER_CODE as STRING) AS cust_tier_code , cast(p.SKU as INT) AS sku,count(*) AS total_no_of_product_views \n" +
                                    "FROM `york-cdf-start.final_input_data.product_views` AS p \n" +
                                    "INNER JOIN `york-cdf-start.final_input_data.customers` AS c \n" +
                                    "ON p.CUSTOMER_ID=c.CUSTOMER_ID \n" +
                                    "GROUP BY cust_tier_code, sku \n" +
                                    "ORDER BY total_no_of_product_views DESC")
                            .usingStandardSql());

        PCollection<TableRow> sales_rows =
                p.apply("Read from BigQuery  orders table",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT cast(c.CUST_TIER_CODE as STRING) as cust_tier_code, cast(o.SKU as INT) AS sku, ROUND(SUM(o.ORDER_AMT), 2) AS total_sales_amount \n" +
                                        "FROM `york-cdf-start.final_input_data.orders` AS o \n" +
                                        "INNER JOIN `york-cdf-start.final_input_data.customers` AS c \n" +
                                        "ON o.CUSTOMER_ID=c.CUSTOMER_ID \n"+
                                        "GROUP BY cust_tier_code, sku \n" +
                                        "ORDER BY total_sales_amount DESC")
                                .usingStandardSql());


//          Writing to BigQuery
        products_view_rows.apply(
                "Write to BigQuery products table",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_soa_xiong.cust_tier_code-sku-total_no_of_product_views_java"))
                        .withSchema(products_schema)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        sales_rows.apply(
                "Write to BigQuery products table",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_soa_xiong.cust_tier_code-sku-total_sales_amount_java"))
                        .withSchema(total_sales_schema)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));


//       pipeline starts to run
        p.run().waitUntilFinish();
    }
}

// COMMANDS TO RUN JAVA CODE
/*
--clean compile code
mvn clean compile

--sets google application credentials to json key--
export GOOGLE_APPLICATION_CREDENTIALS="/Users/yorkmac048/IdeaProjects/bigQuery_dataflow_with_java/src/main/york-cdf-start-8a26c05b158d.json"

--sets environment variable for JAVA--
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.12.jdk/Contents/Home

--runs main class
mvn compile exec:java -Dexec.mainClass=bigQuery_dataflow
*/
