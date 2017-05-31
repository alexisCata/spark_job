import sys
from datetime import datetime
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType, StringType, FloatType, TimestampType
from functions import *


EUR = 'EUR'
# udf_currency_exchange = func.udf(currency_exchange, DecimalType(15, 3)) # replace with exchange_amount (cristobal)
udf_currency_exchange = func.udf(exchange_amount_decimal, FloatType())
udf_calculate_night_amount = func.udf(calculate_night_amount, DecimalType(15, 3))
# udf_round_ccy = func.udf(round_ccy, DecimalType()
udf_round_ccy = func.udf(round_ccy, StringType())  # The results have different number of decimals
udf_date_to_datetime = func.udf(date_to_datetime, TimestampType())

# tables inmemory
df_vta = None
df_cpa = None
df_bok = None
df_discount = None
df_campaignt = None
df_hotelt = None
df_circuitt = None
df_othert = None
df_transfert = None
df_endowt = None
df_extrat = None


def join_all_fields(df_query, df_fields2):#, df_fields3, df_fields4, df_fields5, df_fields6):
    df_r1 = df_query.join(df_fields2, [df_query.operative_incoming == df_fields2.operative_incoming,
                                       df_query.booking_id == df_fields2.booking_id]) \
        .drop(df_fields2.operative_incoming).drop(df_fields2.booking_id).drop(df_fields2.invoicing_company) \
        .drop(df_fields2.creation_date).drop(df_fields2.booking_currency)

    # df_r2 = df_r1.join(df_fields3, [df_r1.operative_incoming == df_fields3.operative_incoming,
    #                                 df_r1.booking_id == df_fields3.booking_id]) \
    #     .drop(df_fields3.operative_incoming).drop(df_fields3.booking_id).drop(df_fields3.invoicing_company) \
    #     .drop(df_fields3.creation_date).drop(df_fields3.booking_currency)
    #
    # df_r3 = df_r2.join(df_fields4, [df_r2.operative_incoming == df_fields4.operative_incoming,
    #                                 df_r2.booking_id == df_fields4.booking_id]) \
    #     .drop(df_fields4.operative_incoming).drop(df_fields4.booking_id).drop(df_fields4.invoicing_company) \
    #     .drop(df_fields4.creation_date).drop(df_fields4.booking_currency)
    #
    # df_r4 = df_r3.join(df_fields5, [df_r3.operative_incoming == df_fields5.operative_incoming,
    #                                 df_r3.booking_id == df_fields5.booking_id]) \
    #     .drop(df_fields5.operative_incoming).drop(df_fields5.booking_id).drop(df_fields5.invoicing_company) \
    #     .drop(df_fields5.creation_date).drop(df_fields5.booking_currency)
    #
    # df_r5 = df_r4.join(df_fields6, [df_r4.operative_incoming == df_fields6.operative_incoming,
    #                                 df_r4.booking_id == df_fields6.booking_id]) \
    #     .drop(df_fields6.operative_incoming).drop(df_fields6.booking_id).drop(df_fields6.invoicing_company) \
    #     .drop(df_fields6.creation_date).drop(df_fields6.booking_currency)

    return df_r1


def change_date_type(dataframe):
    dataframe = dataframe.withColumn("creation_date", udf_date_to_datetime(dataframe.creation_date))
    dataframe = dataframe.withColumn("modification_date", udf_date_to_datetime(dataframe.modification_date))
    dataframe = dataframe.withColumn("cancellation_date", udf_date_to_datetime(dataframe.cancellation_date))
    dataframe = dataframe.withColumn("status_date", udf_date_to_datetime(dataframe.status_date))
    dataframe = dataframe.withColumn("booking_service_from", udf_date_to_datetime(dataframe.booking_service_from))
    dataframe = dataframe.withColumn("booking_service_to", udf_date_to_datetime(dataframe.booking_service_to))

    return dataframe


def load_tables_inmemory(manager):
    global df_vta, df_cpa
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])
    df_vta.cache()
    df_cpa.cache()

    global df_bok, df_discount, df_campaignt
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaignt = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])
    df_bok.cache()
    df_discount.cache()
    df_campaignt.cache()

    global df_hotelt, df_circuitt, df_othert, df_transfert, df_endowt, df_extrat
    df_hotelt = manager.get_dataframe(tables['dwc_bok_t_canco_hotel'])
    df_circuitt = manager.get_dataframe(tables['dwc_bok_t_canco_hotel_circuit'])
    df_othert = manager.get_dataframe(tables['dwc_bok_t_canco_other'])
    df_transfert = manager.get_dataframe(tables['dwc_bok_t_canco_transfer'])
    df_endowt = manager.get_dataframe(tables['dwc_bok_t_canco_endowments'])
    df_extrat = manager.get_dataframe(tables['dwc_bok_t_canco_extra'])
    df_hotelt.cache()
    df_circuitt.cache()
    df_othert.cache()
    df_transfert.cache()
    df_endowt.cache()
    df_extrat.cache()


def direct(manager):
    # get direct dataframes from tables
    df_direct_fields = manager.get_dataframe("hbgdwc.svd_interface_booking_v6")
    df_direct_fields = df_direct_fields.filter(df_direct_fields.creation_date ==
                                               datetime.strptime(sys.argv[1], '%Y-%m-%d').date())

    df_direct_fields = remove_fields(df_direct_fields)

    df_direct_fields = df_direct_fields.limit(100)

    return df_direct_fields


def get_seq_lists(dataframe_bookings):
    """
    Returns two lists with the ids of grec_seq_rec and seq_reserva
    :param dataframe_bookings: 
    :return: list, list
    """
    seq_rec = dataframe_bookings.select('operative_incoming').collect()
    seq_reserva = dataframe_bookings.select('booking_id').collect()
    seq_rec = [val[0] for val in list(seq_rec)]
    seq_reserva = [val[0] for val in list(seq_reserva)]

    return seq_rec, seq_reserva


def sub_tax_sales_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("impuesto_canal",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_venta + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))))) \
        .select("seq_rec", "seq_reserva", "impuesto_canal")

    dataframe = dataframe.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("impuesto_canal",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_venta + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    # FILTER WHERE IN
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.withColumnRenamed("seq_rec", "grec_seq_rec")

    dataframe = dataframe.groupBy("grec_seq_rec", "seq_reserva").agg({'impuesto_canal': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")

    return dataframe


def sub_tax_sales_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    """
    It calculates the subquery for the field Tax_Sales_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: dataframe
    """
    # df_hotel = manager.get_dataframe(tables['dwc_bok_t_canco_hotel'])
    # df_circuit = manager.get_dataframe(tables['dwc_bok_t_canco_hotel_circuit'])
    # df_other = manager.get_dataframe(tables['dwc_bok_t_canco_other'])
    # df_transfer = manager.get_dataframe(tables['dwc_bok_t_canco_transfer'])
    # df_endow = manager.get_dataframe(tables['dwc_bok_t_canco_endowments'])
    # df_extra = manager.get_dataframe(tables['dwc_bok_t_canco_extra'])

    df_aux = df_fields.select("operative_incoming", "booking_id")

    df_hotel = sub_tax_sales_transfer_pricing_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_sales_transfer_pricing_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_sales_transfer_pricing_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_sales_transfer_pricing_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_sales_transfer_pricing_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_sales_transfer_pricing_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)

    df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canal = df_impuesto_canal.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "Tax_Sales_Transfer_pricing")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.operative_incoming == df_impuesto_canal.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canal.seq_reserva],
                               'left_outer').drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_reserva)

    df_fields = df_fields.na.fill({"Tax_Sales_Transfer_pricing": 0})

    df_fields = df_fields.withColumn("Tax_Sales_Transfer_pricing",
                                     udf_round_ccy(df_fields.Tax_Sales_Transfer_pricing,
                                                   df_fields.booking_currency))

    del df_hotel, df_circuit, df_other, df_transfer, df_endow, df_extra, df_impuesto_canal

    return df_fields


def sub_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal",
                "operative_incoming", "booking_id", "booking_currency", "creation_date")

    dataframe = dataframe.withColumn("impuesto_canco1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))))
                                     .otherwise(
                                         dataframe.imp_venta * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
                                             dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))
                                     ))

    dataframe = dataframe.withColumn("impuesto_canco2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    dataframe = dataframe.withColumn('impuesto_canco2', udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.impuesto_canco2
                                                                              ))

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco1", "impuesto_canco2")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco1': 'sum',
                                                                           'impuesto_canco2': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco1)", "impuesto_canco1") \
        .withColumnRenamed("SUM(impuesto_canco2)", "impuesto_canco2")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     dataframe.impuesto_canco1 + dataframe.impuesto_canco2) \
        .drop("impuesto_canco1", "impuesto_canco2").na.fill({"impuesto_canco": 0})

    return dataframe


def sub_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal", "ord_extra",
                "operative_incoming", "booking_id", "booking_currency", "creation_date")

    dataframe = dataframe.withColumn("impuesto_canco2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("impuesto_canco1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))))
                                     .otherwise(
                                         dataframe.imp_venta * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100)))) - (
                                             dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                             1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))
                                     ))

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    # FILTER WHERE IN
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.na.fill({'impuesto_canco1': 0, 'impuesto_canco2': 0})

    # join with the main ids "df_fields"
    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.withColumn('impuesto_canco2', udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.impuesto_canco2))

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco1", "impuesto_canco2")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco1': 'sum',
                                                                           'impuesto_canco2': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco1)", "impuesto_canco1") \
        .withColumnRenamed("SUM(impuesto_canco2)", "impuesto_canco2")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     dataframe.impuesto_canco1 + dataframe.impuesto_canco2) \
        .drop("impuesto_canco1", "impuesto_canco2").na.fill({"impuesto_canco": 0})

    return dataframe


def sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, eur=None):
    df_ids = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date",
                              "booking_currency")  # ids main select fields

    df_cost = manager.get_dataframe(tables["dwc_bok_t_cost"])
    df_cost = df_cost.filter(df_cost.ind_tipo_registro == 'DR').filter(df_cost.ind_facturable == 'S') \
        .select("grec_seq_rec", "rres_seq_reserva", "rext_ord_extra", "sdiv_cod_divisa", "fec_desde", "fec_hasta",
                "nro_unidades", "nro_pax", "ind_tipo_unid", "ind_p_s", "imp_unitario")

    # FILTER NOT EXISTS
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")
    df_filter = df_filter.filter(df_filter.grec_seq_rec.isin(seq_recs)).filter(
        df_filter.rres_seq_reserva.isin(seq_reservas))

    # need ids to exclude

    df_aux_filter = df_cost.join(df_filter, (df_cost.grec_seq_rec == df_filter.grec_seq_rec) &
                                 (df_cost.rres_seq_reserva == df_filter.rres_seq_reserva) &
                                 (df_cost.rext_ord_extra == df_filter.ord_extra), 'left_outer') \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False)) \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra)

    df_res = df_cost.join(df_aux_filter, (df_cost.grec_seq_rec != df_aux_filter.grec_seq_rec) &
                          (df_cost.rres_seq_reserva != df_aux_filter.rres_seq_reserva) &
                          (df_cost.rext_ord_extra != df_aux_filter.ord_extra),
                          'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    df_res = df_res.join(df_ids, [df_res.grec_seq_rec == df_ids.operative_incoming,
                                  df_res.rres_seq_reserva == df_ids.booking_id]) \
        .select(df_ids.operative_incoming, df_ids.booking_id, df_ids.invoicing_company, df_ids.creation_date,
                df_ids.booking_currency,
                df_res.rext_ord_extra, df_res.sdiv_cod_divisa, df_res.fec_desde, df_res.fec_hasta,
                df_res.nro_unidades,
                df_res.nro_pax, df_res.ind_tipo_unid, df_res.ind_p_s, df_res.imp_unitario)

    df_res = df_res.withColumn("night_amount", udf_calculate_night_amount(df_res.fec_desde,
                                                                          df_res.fec_hasta,
                                                                          df_res.nro_unidades,
                                                                          df_res.nro_pax,
                                                                          df_res.ind_tipo_unid,
                                                                          df_res.ind_p_s,
                                                                          df_res.imp_unitario))

    if not eur:
        # Tax_Cost_Transfer_pricing
        df_res = df_res.withColumn('add_impuesto_canco',
                                   udf_currency_exchange(df_res.sdiv_cod_divisa,
                                                         df_res.booking_currency,
                                                         df_res.creation_date,
                                                         df_res.night_amount))
    else:
        # Tax_Transfer_pricing_EUR
        df_res = df_res.withColumn('add_impuesto_canco',
                                   udf_currency_exchange(df_res.sdiv_cod_divisa,
                                                         func.lit(EUR),
                                                         df_res.creation_date,
                                                         df_res.night_amount))

    df_res = df_res.na.fill({'add_impuesto_canco': 0})

    df_addcanco = df_res.groupBy("operative_incoming", "booking_id") \
        .agg({'add_impuesto_canco': 'sum'}).withColumnRenamed("SUM(add_impuesto_canco)", "add_impuesto_canco")

    return df_addcanco


def sub_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    """
    It calculates the subquery for the field Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: dataframe
    """
    # df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    # df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    # df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    # df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    # df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    # df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company",
                              "creation_date", "booking_currency")

    df_hotel = sub_transfer_pricing_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_transfer_pricing_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
    df_other = sub_transfer_pricing_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_transfer_pricing_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
    df_endow = sub_transfer_pricing_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
    df_extra = sub_transfer_pricing_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("operative_incoming", "booking_id") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add impuesto_canco
    df_fields = df_fields.join(df_impuesto_canco, [df_fields.operative_incoming == df_impuesto_canco.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas)

    df_addcanco = df_addcanco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.operative_incoming == df_addcanco.seq_rec,
                                             df_fields.booking_id == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Transfer_pricing", df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    df_fields = df_fields.withColumn("Transfer_pricing", udf_round_ccy(df_fields.Transfer_pricing,
                                                                       df_fields.booking_currency))

    del df_hotel, df_circuit, df_other, df_transfer, df_endow, df_extra, df_impuesto_canco, df_addcanco, df_aux

    return df_fields


def sub_tax_cost_transfer_pricing_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "imp_margen_canco",
                "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "operative_incoming", "booking_id")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con", "ind_tipo_regimen_fac", "imp_margen_canco",
                "imp_venta", "imp_coste", "pct_impuesto_vta", "pct_impuesto_cpa", "imp_margen_canal", "ord_extra",
                "operative_incoming", "booking_id")

    dataframe = dataframe.withColumn("impuesto_canco",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    # FILTER WHERE IN
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    # join with the main ids "df_fields"
    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}) \
        .withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing(manager, df_fields, seq_recs, seq_reservas):
    """
    It calculates the subquery for the field Tax_Cost_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: 
    """
    # df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    # df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    # df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    # df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    # df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    # df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date",
                              "booking_currency")

    df_hotel = sub_tax_cost_transfer_pricing_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_cost_transfer_pricing_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_cost_transfer_pricing_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_cost_transfer_pricing_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_cost_transfer_pricing_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_cost_transfer_pricing_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("operative_incoming", "booking_id") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add impuesto_canco
    df_fields = df_fields.join(df_impuesto_canco, [df_fields.operative_incoming == df_impuesto_canco.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas)

    df_addcanco = df_addcanco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.operative_incoming == df_addcanco.seq_rec,
                                             df_fields.booking_id == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Cost_Transfer_pricing",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    df_fields = df_fields.withColumn("Tax_Cost_Transfer_pricing", udf_round_ccy(df_fields.Tax_Cost_Transfer_pricing,
                                                                                df_fields.booking_currency))

    del df_hotel, df_circuit, df_other, df_transfer, df_endow, df_extra, df_impuesto_canco, df_addcanco, df_aux

    return df_fields


#
# def sub_tax_sales_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
#     # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
#     # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])
#
#     dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
#         .filter(dataframe.seq_reserva.isin(seq_reservas))
#
#     dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
#                                         dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
#                                         dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
#                                         dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
#                                         dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
#                                         ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
#                                                 "cod_esquema_vta", "cod_emp_atlas_vta") \
#         .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
#                        dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
#                        dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
#                        dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
#                        dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
#                        ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
#                                "cod_esquema_cpa", "cod_emp_atlas_cpa") \
#         .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal",
#                 "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa")
#
#     dataframe = dataframe.withColumn("amount",
#                                      func.when(dataframe.ind_tipo_regimen_fac == 'E',
#                                                dataframe.imp_margen_canal * (
#                                                    1 - (1 / (1 + (dataframe.pct_impuesto_vta
#                                                                   / 100)))))
#                                      .otherwise(dataframe.imp_venta * (
#                                          1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
#                                                     dataframe.imp_venta - dataframe.imp_margen_canal) * (
#                                                     1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))
#
#     dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
#                                         dataframe.seq_reserva == df_aux.booking_id])
#
#     dataframe = dataframe.na.fill({'amount': 0})
#
#     dataframe = dataframe.withColumn('impuesto_canal', udf_currency_exchange(dataframe.booking_currency,
#                                                                              func.lit(EUR),
#                                                                              dataframe.creation_date,
#                                                                              dataframe.amount))
#
#     dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canal")
#
#     dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canal': 'sum'}) \
#         .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")
#
#     return dataframe
#
#
# def sub_tax_sales_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
#     # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
#     # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])
#
#     dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
#         .filter(dataframe.seq_reserva.isin(seq_reservas))
#
#     dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
#                                         dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
#                                         dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
#                                         dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
#                                         dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
#                                         ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
#                                                 "cod_esquema_vta", "cod_emp_atlas_vta") \
#         .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
#                        dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
#                        dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
#                        dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
#                        dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
#                        ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
#                                "cod_esquema_cpa", "cod_emp_atlas_cpa") \
#         .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal",
#                 "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")
#
#     dataframe = dataframe.withColumn("amount",
#                                      func.when(dataframe.ind_tipo_regimen_fac == 'E',
#                                                dataframe.imp_margen_canal * (
#                                                    1 - (1 / (1 + (dataframe.pct_impuesto_vta
#                                                                   / 100)))))
#                                      .otherwise(dataframe.imp_venta * (
#                                          1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
#                                                     dataframe.imp_venta - dataframe.imp_margen_canal) * (
#                                                     1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))
#
#     # FILTER WHERE IN
#     # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
#     # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
#     # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])
#
#     df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')
#
#     df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
#                                           df_bok.cod_interface == df_discount.cod_interface]) \
#         .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
#                             df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
#                                                                                             "rres_seq_reserva",
#                                                                                             "ord_extra")
#
#     df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
#                                    (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
#                                    (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
#         .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
#         .filter((df_filter.grec_seq_rec.isNull() == False) &
#                 (df_filter.rres_seq_reserva.isNull() == False) &
#                 (df_filter.ord_extra.isNull() == False))
#
#     dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
#                                (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
#                                (dataframe.ord_extra != df_aux_filter.ord_extra),
#                                'left_outer') \
#         .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)
#
#     dataframe = dataframe.na.fill({'amount': 0})
#
#     # join with the main ids "df_fields"
#     dataframe = dataframe.join(df_aux, (dataframe.seq_rec == df_aux.operative_incoming) &
#                                (dataframe.seq_reserva == df_aux.booking_id))
#
#     dataframe = dataframe.withColumn('impuesto_canal', udf_currency_exchange(dataframe.booking_currency,
#                                                                              func.lit(EUR),
#                                                                              dataframe.creation_date,
#                                                                              dataframe.amount))
#
#     dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canal")
#
#     dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canal': 'sum'}) \
#         .withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")
#
#     return dataframe
#
#
# def sub_tax_sales_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
#     """
#     It calculates the subquery for the field Tax_Sales_Transfer_pricing_EUR
#     :param manager:
#     :param df_fields:
#     :param seq_recs:
#     :param seq_reservas:
#     :return:
#     """
#     # df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
#     # df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
#     # df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
#     # df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
#     # df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
#     # df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])
#
#     df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date", "booking_currency")
#
#     df_hotel = sub_tax_sales_transfer_pricing_eur_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
#     df_circuit = sub_tax_sales_transfer_pricing_eur_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
#     df_other = sub_tax_sales_transfer_pricing_eur_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
#     df_transfer = sub_tax_sales_transfer_pricing_eur_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
#     df_endow = sub_tax_sales_transfer_pricing_eur_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
#     df_extra = sub_tax_sales_transfer_pricing_eur_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)
#
#     df_impuesto_canal = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
#         df_extra)
#
#     df_impuesto_canal = df_impuesto_canal.groupBy("operative_incoming", "booking_id") \
#         .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "impuesto_canal")
#
#     df_impuesto_canal = df_impuesto_canal.withColumnRenamed("operative_incoming", "seq_rec") \
#         .withColumnRenamed("booking_id", "seq_res")
#
#     df_fields = df_fields.join(df_impuesto_canal, [df_fields.operative_incoming == df_impuesto_canal.seq_rec,
#                                                    df_fields.booking_id == df_impuesto_canal.seq_res],
#                                "left_outer").drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_res)
#
#     df_fields = df_fields.na.fill({'impuesto_canal': 0}).withColumnRenamed('impuesto_canal',
#                                                                            'Tax_Sales_Transfer_pricing_EUR')
#
#     return df_fields
#


def sub_tax_sales_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    df_fields = df_fields.withColumn('Tax_Sales_Transfer_pricing_EUR',
                                     udf_currency_exchange(df_fields.booking_currency,
                                                           func.lit(EUR),
                                                           df_fields.creation_date,
                                                           df_fields.Tax_Sales_Transfer_pricing))

    df_fields = df_fields.na.fill({'Tax_Sales_Transfer_pricing_EUR': 0})

    return df_fields


def sub_tax_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa",
                "operative_incoming", "booking_id", "booking_currency", "creation_date")

    dataframe = dataframe.withColumn("amount1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount1': 0})
    dataframe = dataframe.na.fill({'amount2': 0})

    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.withColumn("impuesto_canco1", udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.amount1))

    dataframe = dataframe.withColumn("impuesto_canco2", udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.amount2))

    dataframe = dataframe.withColumn("impuesto_canco", dataframe.impuesto_canco1 + dataframe.impuesto_canco2)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    # dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
    #     .filter(dataframe.seq_reserva.isin(seq_reservas))
    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_fac", "imp_margen_canal", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "imp_venta", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra",
                "operative_incoming", "booking_id", "booking_currency", "creation_date")

    dataframe = dataframe.withColumn("amount1",
                                     func.when(dataframe.ind_tipo_regimen_fac == 'E',
                                               dataframe.imp_margen_canal * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise(dataframe.imp_venta * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_venta - dataframe.imp_margen_canal) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.withColumn("amount2",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount1': 0})
    dataframe = dataframe.na.fill({'amount2': 0})

    # FILTER WHERE IN
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    # dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
    #                                     dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.withColumn("impuesto_canco1", udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.amount1))

    dataframe = dataframe.withColumn("impuesto_canco2", udf_currency_exchange(dataframe.booking_currency,
                                                                              func.lit(EUR),
                                                                              dataframe.creation_date,
                                                                              dataframe.amount2))

    dataframe = dataframe.withColumn("impuesto_canco", dataframe.impuesto_canco1 + dataframe.impuesto_canco2)

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    """
    It calculates the subquery for the field Tax_Transfer_pricing_EUR
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: 
    """
    # df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    # df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    # df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    # df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    # df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    # df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date",
                              "booking_currency")

    df_hotel = sub_tax_transfer_pricing_eur_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_transfer_pricing_eur_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_transfer_pricing_eur_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_transfer_pricing_eur_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_transfer_pricing_eur_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_transfer_pricing_eur_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("operative_incoming", "booking_id") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    df_fields = df_fields.join(df_impuesto_canco, [df_fields.operative_incoming == df_impuesto_canco.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, EUR)

    df_addcanco = df_addcanco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.operative_incoming == df_addcanco.seq_rec,
                                             df_fields.booking_id == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Transfer_pricing_EUR",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    del df_hotel, df_circuit, df_other, df_transfer, df_endow, df_extra, df_impuesto_canco, df_addcanco

    return df_fields


def sub_tax_cost_transfer_pricing_eur_aux(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "pct_impuesto_vta", "pct_impuesto_cpa")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount': 0})

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.withColumn("impuesto_canco", udf_currency_exchange(dataframe.booking_currency,
                                                                             func.lit(EUR),
                                                                             dataframe.creation_date,
                                                                             dataframe.amount))

    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_eur_aux_extra(manager, dataframe, seq_recs, seq_reservas, df_aux):
    # df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    # df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])

    dataframe = dataframe.filter(dataframe.seq_rec.isin(seq_recs)) \
        .filter(dataframe.seq_reserva.isin(seq_reservas))

    dataframe = dataframe.join(df_vta, [dataframe.ind_tipo_imp_vta_fac == df_vta.ind_tipo_imp_vta,
                                        dataframe.cod_impuesto_vta_fac == df_vta.cod_impuesto_vta,
                                        dataframe.cod_clasif_vta_fac == df_vta.cod_clasif_vta,
                                        dataframe.cod_esquema_vta_fac == df_vta.cod_esquema_vta,
                                        dataframe.cod_empresa_vta_fac == df_vta.cod_emp_atlas_vta,
                                        ]).drop("ind_tipo_imp_vta", "cod_impuesto_vta", "cod_clasif_vta",
                                                "cod_esquema_vta", "cod_emp_atlas_vta") \
        .join(df_cpa, [dataframe.ind_tipo_imp_vta_fac == df_cpa.ind_tipo_imp_cpa,
                       dataframe.cod_impuesto_vta_fac == df_cpa.cod_impuesto_cpa,
                       dataframe.cod_clasif_vta_fac == df_cpa.cod_clasif_cpa,
                       dataframe.cod_esquema_vta_fac == df_cpa.cod_esquema_cpa,
                       dataframe.cod_empresa_vta_fac == df_cpa.cod_emp_atlas_cpa,
                       ]).drop("ind_tipo_imp_cpa", "cod_impuesto_cpa", "cod_clasif_cpa",
                               "cod_esquema_cpa", "cod_emp_atlas_cpa") \
        .select("seq_rec", "seq_reserva", "ind_tipo_regimen_con",
                "imp_coste", "imp_margen_canco", "pct_impuesto_vta", "pct_impuesto_cpa", "ord_extra")

    dataframe = dataframe.withColumn("amount",
                                     func.when(dataframe.ind_tipo_regimen_con == 'E',
                                               dataframe.imp_margen_canco * (
                                                   1 - (1 / (1 + (dataframe.pct_impuesto_vta
                                                                  / 100)))))
                                     .otherwise((dataframe.imp_coste + dataframe.imp_margen_canco) * (
                                         1 - (1 / (1 + (dataframe.pct_impuesto_vta / 100)))) - (
                                                    dataframe.imp_coste) * (
                                                    1 - (1 / (1 + (dataframe.pct_impuesto_cpa / 100))))))

    dataframe = dataframe.na.fill({'amount': 0})

    # FILTER WHERE IN
    # df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    # df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    # df_campaign = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_campaign = df_campaignt.filter(df_campaignt.ind_rentabilidad == 'N')

    df_filter = df_bok.join(df_discount, [df_bok.num_bono == df_discount.num_bono,
                                          df_bok.cod_interface == df_discount.cod_interface]) \
        .join(df_campaign, [df_discount.cod_campana == df_campaign.cod_campana,
                            df_discount.cod_interface == df_campaign.cod_interface]).select("grec_seq_rec",
                                                                                            "rres_seq_reserva",
                                                                                            "ord_extra")

    df_aux_filter = dataframe.join(df_filter, (dataframe.seq_rec == df_filter.grec_seq_rec) &
                                   (dataframe.seq_reserva == df_filter.rres_seq_reserva) &
                                   (dataframe.ord_extra == df_filter.ord_extra), 'left_outer') \
        .select(df_filter.grec_seq_rec, df_filter.rres_seq_reserva, df_filter.ord_extra) \
        .filter((df_filter.grec_seq_rec.isNull() == False) &
                (df_filter.rres_seq_reserva.isNull() == False) &
                (df_filter.ord_extra.isNull() == False))

    dataframe = dataframe.join(df_aux_filter, (dataframe.seq_rec != df_aux_filter.grec_seq_rec) &
                               (dataframe.seq_reserva != df_aux_filter.rres_seq_reserva) &
                               (dataframe.ord_extra != df_aux_filter.ord_extra),
                               'left_outer') \
        .drop(df_aux_filter.grec_seq_rec).drop(df_aux_filter.rres_seq_reserva).drop(df_aux_filter.ord_extra)

    dataframe = dataframe.join(df_aux, [dataframe.seq_rec == df_aux.operative_incoming,
                                        dataframe.seq_reserva == df_aux.booking_id])

    dataframe = dataframe.withColumn("impuesto_canco", udf_currency_exchange(dataframe.booking_currency,
                                                                             func.lit(EUR),
                                                                             dataframe.creation_date,
                                                                             dataframe.amount))
    dataframe = dataframe.na.fill({'impuesto_canco': 0})

    dataframe = dataframe.select("operative_incoming", "booking_id", "impuesto_canco")

    dataframe = dataframe.groupBy("operative_incoming", "booking_id").agg({'impuesto_canco': 'sum'}). \
        withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    return dataframe


def sub_tax_cost_transfer_pricing_eur(manager, df_fields, seq_recs, seq_reservas):
    """
    It calculates the subquery for the field Tax_Cost_Transfer_pricing_EUR
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: 
    """
    # df_hotel = manager.get_dataframe(tables["dwc_bok_t_canco_hotel"])
    # df_circuit = manager.get_dataframe(tables["dwc_bok_t_canco_hotel_circuit"])
    # df_other = manager.get_dataframe(tables["dwc_bok_t_canco_other"])
    # df_transfer = manager.get_dataframe(tables["dwc_bok_t_canco_transfer"])
    # df_endow = manager.get_dataframe(tables["dwc_bok_t_canco_endowments"])
    # df_extra = manager.get_dataframe(tables["dwc_bok_t_canco_extra"])

    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date",
                              "booking_currency")

    df_hotel = sub_tax_transfer_pricing_eur_aux(manager, df_hotelt, seq_recs, seq_reservas, df_aux)
    df_circuit = sub_tax_transfer_pricing_eur_aux(manager, df_circuitt, seq_recs, seq_reservas, df_aux)
    df_other = sub_tax_transfer_pricing_eur_aux(manager, df_othert, seq_recs, seq_reservas, df_aux)
    df_transfer = sub_tax_transfer_pricing_eur_aux(manager, df_transfert, seq_recs, seq_reservas, df_aux)
    df_endow = sub_tax_transfer_pricing_eur_aux(manager, df_endowt, seq_recs, seq_reservas, df_aux)
    df_extra = sub_tax_transfer_pricing_eur_aux_extra(manager, df_extrat, seq_recs, seq_reservas, df_aux)

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("operative_incoming", "booking_id") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_impuesto_canco = df_impuesto_canco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    df_fields = df_fields.join(df_impuesto_canco, [df_fields.operative_incoming == df_impuesto_canco.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canco.seq_res],
                               'left_outer').drop("seq_rec", "seq_res")

    df_addcanco = sub_transfer_pricing_aux_add_canco(manager, df_fields, seq_recs, seq_reservas, EUR)

    df_addcanco = df_addcanco.withColumnRenamed("operative_incoming", "seq_rec") \
        .withColumnRenamed("booking_id", "seq_res")

    # add add_impuesto_canco
    df_fields = df_fields.join(df_addcanco, [df_fields.operative_incoming == df_addcanco.seq_rec,
                                             df_fields.booking_id == df_addcanco.seq_res],
                               "left_outer").drop(df_addcanco.seq_rec).drop(df_addcanco.seq_res)

    df_fields = df_fields.na.fill({'impuesto_canco': 0, 'add_impuesto_canco': 0})

    df_fields = df_fields.withColumn("Tax_Cost_Transfer_pricing_EUR",
                                     df_fields.impuesto_canco + df_fields.add_impuesto_canco) \
        .drop("impuesto_canco", "add_impuesto_canco")

    del df_hotel, df_circuit, df_other, df_transfer, df_endow, df_extra, df_impuesto_canco, df_addcanco

    return df_fields


def remove_fields(dataframe):
    dataframe = dataframe.drop("Tax_Sales_Transfer_pricing") \
        .drop("Tax_Sales_Transfer_pricing_EUR") \
        .drop("Transfer_pricing") \
        .drop("tax_transfer_pricing_eur") \
        .drop("Tax_Cost_Transfer_pricing") \
        .drop("Tax_Cost_Transfer_pricing_EUR")

    return dataframe

