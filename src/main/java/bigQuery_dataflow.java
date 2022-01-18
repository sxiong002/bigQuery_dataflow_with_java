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
        DataflowPipelineOptions dataflowOptions =
                PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowOptions.setProject("york-cdf-start");
        dataflowOptions.setRegion("us-central1");
        dataflowOptions.setJobName("final_jaya_mohan_capstone");
        dataflowOptions.setRunner(DataflowRunner.class);

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
//        Reading and writing to table.

        PCollection<TableRow> products_view_rows =
                p.apply(
                "Read from BigQuery query",
                BigQueryIO.readTableRows()
                        .fromQuery("select cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE ,p.SKU,count(c.CUSTOMER_ID) as total_no_of_product_views FROM `york-cdf-start.final_input_data.product_views` as p             \n" +
                                "                join `york-cdf-start.final_input_data.customers` as c \n" +
                                "                on p.CUSTOMER_ID=c.CUSTOMER_ID \n" +
                                "                group by c.CUST_TIER_CODE,p.SKU")
                        .usingStandardSql());
        PCollection<TableRow> sales_rows =
                p.apply(
                        "Read from BigQuery  orders table",
                        BigQueryIO.readTableRows()
                                .fromQuery("SELECT cast(c.CUST_TIER_CODE as STRING) as CUST_TIER_CODE,o.SKU,sum(o.ORDER_AMT) as total_sales_amount  FROM `york-cdf-start.final_input_data.orders` as o \n" +
                                        "                join `york-cdf-start.final_input_data.customers` as c \n" +
                                        "                on o.CUSTOMER_ID=c.CUSTOMER_ID group by o.SKU,c.CUST_TIER_CODE")
                                .usingStandardSql());


        products_view_rows.apply(
                "Write to BigQuery products table",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_jaya_mohan.cust_tier_code-sku-total_no_of_product_java"))
                        .withSchema(products_schema)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        sales_rows.apply(
                "Write to BigQuery products table",
                BigQueryIO.writeTableRows()
                        .to(String.format("york-cdf-start:final_jaya_mohan.cust_tier_code-sku-total_sales_amount_java"))
                        .withSchema(total_sales_schema)

                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
//       pipeline starts to run
        p.run().waitUntilFinish();
    }
}
