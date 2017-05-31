from time import time
from datetime import date, datetime
from settings import logger
from manager.spark_manager import SparkManager
from iface.select_fields import *

# /opt/conf/spark-defaults.conf
# #RedshiftJDBC42-1.2.1.1001.jar  http://docs.aws.amazon.com/redshift/latest/mgmt/configure-jdbc-connection.html#download-jdbc-driver
# #spark-avro_2.11-3.2.0.jar      https://spark-packages.org/package/databricks/spark-avro

# spark.driver.extraClassPath /home/alexis/Downloads/jars/*
# spark.executor.extraClassPath /home/alexis/Downloads/jars/*
# spark.sql.broadcastTimeout 600


#findspark commented due to errors in the cluster (spark_maanger, settings)


if __name__ == "__main__":
    logger.info("START TIME: {}".format(datetime.now()))

    spark_manager = SparkManager(URL, DRIVER, USER, PASS)

    t00 = time()
    logger.info("Start main query: {}".format(datetime.now()))

    df_query = direct(spark_manager)
    df_query.cache()

    tt = time() - t00
    logger.info("Elapsed query: {}".format(tt))
    logger.info("End main query: {}".format(datetime.now()))

    df_fields = df_query.select("operative_incoming", "booking_id", "invoicing_company",
                                "creation_date", "booking_currency")
    df_fields.cache()
    df_fields.createOrReplaceTempView("main")

    load_tables_inmemory(spark_manager)
    #
    t0 = time()
    logger.info("Start time fields: {}".format(datetime.now()))
    df_fields1 = sub_tax_sales_transfer_pricing(spark_manager, df_fields)
    df_fields1.cache()
    logger.info("DONE TAX_SALES_TRANSFER_PRICING: {}".format(datetime.now()))

    df_fields2 = sub_tax_sales_transfer_pricing_eur(df_fields1)
    df_fields2.cache()
    logger.info("DONE TAX_SALES_TRANSFER_PRICING_EUR: {}".format(datetime.now()))

    df_fields3 = sub_tax_cost_transfer_pricing(spark_manager, df_fields)
    df_fields3.cache()
    logger.info("DONE TAX_COST_TRANSFER_PRICING: {}".format(datetime.now()))

    df_fields4 = sub_tax_cost_transfer_pricing_eur(df_fields3)
    df_fields4.cache()
    logger.info("DONE TAX_COST_TRANSFER_PRICING_EUR: {}".format(datetime.now()))

    df_fields5 = sub_transfer_pricing(spark_manager, df_fields)
    df_fields5.cache()
    logger.info("DONE TRANSFER_PRICING: {}".format(datetime.now()))

    df_fields6 = sub_tax_transfer_pricing_eur(df_fields5)
    df_fields6.cache()
    logger.info("DONE TAX_TRANSFER_PRICING_EUR: {}".format(datetime.now()))

    df_result = join_all_fields(df_query, df_fields2, df_fields4, df_fields6)
    df_result.cache()

    tt = time() - t0
    logger.info("Elapsed time fields: {}".format(tt))
    logger.info("End time fields: {}".format(datetime.now()))

    t1 = time()

    df_fields = change_date_type(df_result)

    logger.info("Start time create file: {}".format(datetime.now()))
    df_fields.write.format("com.databricks.spark.avro").save('daily_iface_extractions_{}'.format(date.today()))
    # df_fields.write.format("com.databricks.spark.csv").save('daily_iface_extractions_{}'.format(date.today()))
    tt = time() - t1
    logger.info("Elapsed time creating file: {}".format(tt))
    logger.info("End time create file: {}".format(datetime.now()))

    logger.info("END TIME: {}".format(datetime.now()))

    spark_manager.stop_session()
