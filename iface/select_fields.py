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

# df_vta = None
# df_cpa = None
# df_bok = None
# df_discount = None
# df_campaignt = None
# df_hotelt = None
# df_circuitt = None
# df_othert = None
# df_transfert = None
# df_endowt = None
# df_extrat = None
# df = None


def load_tables_inmemory(manager):
    manager.session.udf.register("decode", decode_sql)
    manager.session.udf.register("replace", replace_sql)
    manager.session.udf.register("round", round_sql)
    manager.session.udf.register("exchange_rate", exchange_amount)
    manager.session.udf.register("night_amount", calculate_night_amount)
    manager.session.udf.register("round_ccy", round_ccy)

    global df_vta, df_cpa, df
    df_vta = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_vta'])
    df_cpa = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap_cpa'])
    df_vta.cache()
    df_cpa.cache()

    df = manager.get_dataframe(tables['dwc_oth_v_re_v_impuesto_sap'])
    df.createOrReplaceTempView("impuesto_sap")
    df.cache()

    global df_bok, df_discount, df_campaignt
    df_bok = manager.get_dataframe(tables['dwc_bok_t_extra'])
    df_discount = manager.get_dataframe(tables['dwc_cli_dir_t_cd_discount_bond'])
    df_campaignt = manager.get_dataframe(tables['dwc_cli_dir_t_cd_campaign'])

    df_bok.createOrReplaceTempView("bok_extra")
    df_discount.createOrReplaceTempView("discount_bond")
    df_campaignt.createOrReplaceTempView("campaign")
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
    df_cost = manager.get_dataframe(tables['dwc_bok_t_cost'])

    df_hotelt.createOrReplaceTempView("hotel")
    df_circuitt.createOrReplaceTempView("circuit")
    df_othert.createOrReplaceTempView("other")
    df_transfert.createOrReplaceTempView("transfer")
    df_endowt.createOrReplaceTempView("endowments")
    df_extrat.createOrReplaceTempView("extra")
    df_cost.createOrReplaceTempView("cost")

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


def sub_tax_sales_transfer_pricing_aux(manager, table):

    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_fac,"
                                 "                   'E', imp_margen_canal"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   imp_venta"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_venta - imp_margen_canal)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) impuesto_canal "
                                 "    from {}, "
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "   GROUP BY seq_rec, seq_reserva".format(table))

    return df_res


def sub_tax_sales_transfer_pricing_aux_extra(manager, table):
    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_fac,"
                                 "                   'E', imp_margen_canal"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   imp_venta"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_venta - imp_margen_canal)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) impuesto_canal "
                                 "    from {} e,"
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "     AND not exists  (SELECT * "
                                 "                        FROM bok_extra EXT,"
                                 "                             discount_bond BON,"
                                 "                             campaign CAM"
                                 "                       WHERE e.seq_rec = EXT.grec_seq_rec"
                                 "                         AND e.seq_reserva = EXT.rres_seq_reserva "
                                 "                         AND e.ord_extra = EXT.ord_extra"
                                 "                         AND EXT.num_bono = BON.num_bono"
                                 "                         AND EXT.cod_interface = BON.cod_interface"
                                 "                         AND BON.cod_campana = CAM.cod_campana"
                                 "                         AND BON.cod_interface = CAM.cod_interface"
                                 "                         AND CAM.ind_rentabilidad = 'N')"
                                 "    GROUP BY seq_rec, seq_reserva".format(table))

    return df_res


def sub_tax_sales_transfer_pricing(manager, df_fields):
    """
    It calculates the subquery for the field Tax_Sales_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: dataframe
    """
    df_aux = df_fields.select("operative_incoming", "booking_id")
    df_aux.cache()
    df_aux.createOrReplaceTempView("aux")

    df_hotel = sub_tax_sales_transfer_pricing_aux(manager, "hotel")
    df_circuit = sub_tax_sales_transfer_pricing_aux(manager, "circuit")
    df_other = sub_tax_sales_transfer_pricing_aux(manager, "other")
    df_transfer = sub_tax_sales_transfer_pricing_aux(manager, "transfer")
    df_endow = sub_tax_sales_transfer_pricing_aux(manager, "endowments")
    df_extra = sub_tax_sales_transfer_pricing_aux_extra(manager, "extra")

    df_impuesto_can = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(df_extra)

    df_impuesto_canal = df_impuesto_can.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canal': 'sum'}).withColumnRenamed("SUM(impuesto_canal)", "tax_sales_transfer_pricing")

    df_fields = df_fields.join(df_impuesto_canal, [df_fields.operative_incoming == df_impuesto_canal.seq_rec,
                                                   df_fields.booking_id == df_impuesto_canal.seq_reserva],
                               'left_outer').drop(df_impuesto_canal.seq_rec).drop(df_impuesto_canal.seq_reserva)

    df_fields1 = df_fields.na.fill({"tax_sales_transfer_pricing": 0})

    df_fields2 = df_fields1.withColumn("tax_sales_transfer_pricing",
                                     udf_round_ccy(df_fields1.tax_sales_transfer_pricing,
                                                   df_fields1.booking_currency))

    return df_fields2


def sub_transfer_pricing_aux(manager, dataframe):

    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       (SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_fac,"
                                 "                   'E', imp_margen_canal"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   imp_venta"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_venta - imp_margen_canal)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) "
                                 "       + SUM ("
                                 "               NVL ("
                                 "                  exchange_rate ("
                                 "                     r.booking_currency,"
                                 "                     'EUR',"
                                 "                     r.creation_date,"
                                 "                     DECODE ("
                                 "                        ind_tipo_regimen_con,"
                                 "                        'E', imp_margen_canco"
                                 "                             * (1"
                                 "                                - (1"
                                 "                                   / (1"
                                 "                                      + (vta.pct_impuesto"
                                 "                                         / 100)))),"
                                 "                        (imp_coste + imp_margen_canco)"
                                 "                        * (1"
                                 "                           - (1"
                                 "                              / (1"
                                 "                                 + (vta.pct_impuesto / 100))))"
                                 "                        - (imp_coste)"
                                 "                          * (1"
                                 "                             - (1"
                                 "                                / (1"
                                 "                                   + (cpa.pct_impuesto / 100)))))),"
                                 "                  0))"
                                 " ) impuesto_canco"
                                 "    from {}, "
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "   GROUP BY seq_rec, seq_reserva".format(dataframe))

    return df_res


def sub_transfer_pricing_aux_extra(manager, dataframe):

    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       (SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_fac,"
                                 "                   'E', imp_margen_canal"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   imp_venta"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_venta - imp_margen_canal)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) "
                                 "       + SUM ("
                                 "               NVL ("
                                 "                  exchange_rate ("
                                 "                     r.booking_currency,"
                                 "                     'EUR',"
                                 "                     r.creation_date,"
                                 "                     DECODE ("
                                 "                        ind_tipo_regimen_con,"
                                 "                        'E', imp_margen_canco"
                                 "                             * (1"
                                 "                                - (1"
                                 "                                   / (1"
                                 "                                      + (vta.pct_impuesto"
                                 "                                         / 100)))),"
                                 "                        (imp_coste + imp_margen_canco)"
                                 "                        * (1"
                                 "                           - (1"
                                 "                              / (1"
                                 "                                 + (vta.pct_impuesto / 100))))"
                                 "                        - (imp_coste)"
                                 "                          * (1"
                                 "                             - (1"
                                 "                                / (1"
                                 "                                   + (cpa.pct_impuesto / 100)))))),"
                                 "                  0))"
                                 " ) impuesto_canco"
                                 "    from {} e, "
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "     AND not exists  (SELECT * "
                                 "                        FROM bok_extra EXT,"
                                 "                             discount_bond BON,"
                                 "                             campaign CAM"
                                 "                       WHERE e.seq_rec = EXT.grec_seq_rec"
                                 "                         AND e.seq_reserva = EXT.rres_seq_reserva "
                                 "                         AND e.ord_extra = EXT.ord_extra"
                                 "                         AND EXT.num_bono = BON.num_bono"
                                 "                         AND EXT.cod_interface = BON.cod_interface"
                                 "                         AND BON.cod_campana = CAM.cod_campana"
                                 "                         AND BON.cod_interface = CAM.cod_interface"
                                 "                         AND CAM.ind_rentabilidad = 'N')"
                                 "    GROUP BY seq_rec, seq_reserva".format(dataframe))

    return df_res


def sub_transfer_pricing(manager, df_fields):
    """
    It calculates the subquery for the field Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: dataframe
    """
    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company",
                              "creation_date", "booking_currency")
    df_aux.cache()
    df_aux.createOrReplaceTempView("aux")

    df_hotel = sub_transfer_pricing_aux(manager, "hotel")
    df_circuit = sub_transfer_pricing_aux(manager, "circuit")
    df_other = sub_transfer_pricing_aux(manager, "other")
    df_transfer = sub_transfer_pricing_aux(manager, "transfer")
    df_endow = sub_transfer_pricing_aux(manager, "endowments")
    df_extra = sub_transfer_pricing_aux_extra(manager, "extra")

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("seq_rec", "seq_reserva") \
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_addcanco = manager.session.sql(
        "SELECT operative_incoming, booking_id,"
        "        NVL (SUM (exchange_rate ( cst.sdiv_cod_divisa,"
        "                                   r.booking_currency,"
        "                                   r.creation_date,"
        "                                   night_amount ("
        "                                      cst.fec_desde,"
        "                                      cst.fec_hasta,"
        "                                      cst.nro_unidades,"
        "                                      cst.nro_pax,"
        "                                      cst.ind_tipo_unid,"
        "                                      cst.ind_p_s,"
        "                                      cst.imp_unitario))),"
        "                           0) cost_field"
        "                 FROM cost cst, aux r"
        "                WHERE     cst.grec_seq_rec = r.operative_incoming"
        "                      AND cst.rres_seq_reserva = r.booking_id"
        "                      AND cst.ind_tipo_registro = 'DR'"
        "                      AND cst.ind_facturable = 'S'"
        "                      AND NOT EXISTS"
        "                                 (SELECT 1"
        "                                    FROM bok_extra EXT,"
        "                                         discount_bond BON,"
        "                                         campaign CAM"
        "                                   WHERE CST.grec_seq_rec = EXT.grec_seq_rec"
        "                                         AND CST.rres_seq_reserva = EXT.rres_seq_reserva"
        "                                         AND CST.rext_ord_extra = EXT.ord_extra"
        "                                         AND EXT.num_bono = BON.num_bono"
        "                                         AND EXT.cod_interface = BON.cod_interface"
        "                                         AND BON.cod_campana = CAM.cod_campana"
        "                                         AND BON.cod_interface = CAM.cod_interface"
        "                                         AND CAM.ind_rentabilidad = 'N')"
        " group by operative_incoming, booking_id")

    df_aux.select("operative_incoming", "booking_id", "booking_currency").createOrReplaceTempView("aux")
    df_impuesto_canco.createOrReplaceTempView("canco")
    df_addcanco.createOrReplaceTempView("add_canco")

    df_r = manager.session.sql("select F.operative_incoming, F.booking_id, "
                               "       round_ccy(NVL(SUM(impuesto_canco + cost_field), 0), F.booking_currency) "
                               "       transfer_pricing"
                               " FROM aux F LEFT JOIN canco C "
                               "   ON F.operative_incoming = C.seq_rec"
                               "  AND F.booking_id = C.seq_reserva"
                               "  LEFT JOIN add_canco A "
                               "   ON F.operative_incoming = A.operative_incoming"
                               "  AND F.booking_id = A.booking_id"
                               " GROUP BY F.operative_incoming, F.booking_id, F.booking_currency")

    df_res = df_fields.join(df_r, ["operative_incoming", "booking_id"]) \
        .select(df_fields.operative_incoming, df_fields.booking_id, df_fields.creation_date,
                df_fields.booking_currency, df_r.transfer_pricing)

    return df_res


def sub_tax_cost_transfer_pricing_aux(manager, dataframe):

    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_con,"
                                 "                   'E', imp_margen_canco"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   (imp_coste + imp_margen_canco)"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_coste)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) impuesto_canco "
                                 "    from {} e,"
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "   GROUP BY seq_rec, seq_reserva".format(dataframe))

    return df_res


def sub_tax_cost_transfer_pricing_aux_extra(manager, table):
    df_res = manager.session.sql("select seq_rec, seq_reserva, "
                                 "       SUM ("
                                 "             NVL ("
                                 "                DECODE ("
                                 "                   ind_tipo_regimen_con,"
                                 "                   'E', imp_margen_canco"
                                 "                        * (1"
                                 "                           - (1 / (1 + (vta.pct_impuesto / 100)))),"
                                 "                   (imp_coste + imp_margen_canco)"
                                 "                   * (1 - (1 / (1 + (vta.pct_impuesto / 100))))"
                                 "                   - (imp_coste)"
                                 "                     * (1 - (1 / (1 + (cpa.pct_impuesto / 100))))),"
                                 "                0)) impuesto_canco "
                                 "    from {} e,"
                                 "         aux r, "
                                 "         impuesto_sap vta, "
                                 "         impuesto_sap cpa"
                                 "   where seq_rec = r.operative_incoming"
                                 "     and seq_reserva = r.booking_id"
                                 "     and cod_impuesto_vta_fac = vta.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = vta.cod_clasif"
                                 "     AND cod_esquema_vta_fac = vta.cod_esquema"
                                 "     AND cod_empresa_vta_fac = vta.cod_emp_atlas"
                                 "     AND ind_tipo_imp_vta_fac = cpa.ind_tipo_imp"
                                 "     AND cod_impuesto_vta_fac = cpa.cod_impuesto"
                                 "     AND cod_clasif_vta_fac = cpa.cod_clasif"
                                 "     AND cod_esquema_vta_fac = cpa.cod_esquema"
                                 "     AND cod_empresa_vta_fac = cpa.cod_emp_atlas"
                                 "     AND not exists  (SELECT * "
                                 "                        FROM bok_extra EXT,"
                                 "                             discount_bond BON,"
                                 "                             campaign CAM"
                                 "                       WHERE e.seq_rec = EXT.grec_seq_rec"
                                 "                         AND e.seq_reserva = EXT.rres_seq_reserva "
                                 "                         AND e.ord_extra = EXT.ord_extra"
                                 "                         AND EXT.num_bono = BON.num_bono"
                                 "                         AND EXT.cod_interface = BON.cod_interface"
                                 "                         AND BON.cod_campana = CAM.cod_campana"
                                 "                         AND BON.cod_interface = CAM.cod_interface"
                                 "                         AND CAM.ind_rentabilidad = 'N')"
                                 "    GROUP BY seq_rec, seq_reserva".format(table))

    return df_res


def sub_tax_cost_transfer_pricing(manager, df_fields):
    """
    It calculates the subquery for the field Tax_Cost_Transfer_pricing 
    :param manager: 
    :param df_fields: 
    :param seq_recs: 
    :param seq_reservas: 
    :return: 
    """

    df_aux = df_fields.select("operative_incoming", "booking_id", "invoicing_company", "creation_date",
                              "booking_currency")
    df_aux.createOrReplaceTempView("aux")

    df_hotel = sub_tax_cost_transfer_pricing_aux(manager, "hotel")
    df_circuit = sub_tax_cost_transfer_pricing_aux(manager, "circuit")
    df_other = sub_tax_cost_transfer_pricing_aux(manager, "other")
    df_transfer = sub_tax_cost_transfer_pricing_aux(manager, "transfer")
    df_endow = sub_tax_cost_transfer_pricing_aux(manager, "endowments")
    df_extra = sub_tax_cost_transfer_pricing_aux_extra(manager, "extra")

    df_impuesto_canco = df_hotel.union(df_circuit).union(df_other).union(df_transfer).union(df_endow).union(
        df_extra)

    df_impuesto_canco = df_impuesto_canco.groupBy("seq_rec", "seq_reserva")\
        .agg({'impuesto_canco': 'sum'}).withColumnRenamed("SUM(impuesto_canco)", "impuesto_canco")

    df_addcanco = manager.session.sql(
                            "SELECT operative_incoming, booking_id,"
                            "        NVL (SUM (exchange_rate ( cst.sdiv_cod_divisa,"
                            "                                   r.booking_currency,"
                            "                                   r.creation_date,"
                            "                                   night_amount ("
                            "                                      cst.fec_desde,"
                            "                                      cst.fec_hasta,"
                            "                                      cst.nro_unidades,"
                            "                                      cst.nro_pax,"
                            "                                      cst.ind_tipo_unid,"
                            "                                      cst.ind_p_s,"
                            "                                      cst.imp_unitario))),"
                            "                           0) cost_field"
                            "                 FROM cost cst, aux r"
                            "                WHERE     cst.grec_seq_rec = r.operative_incoming"
                            "                      AND cst.rres_seq_reserva = r.booking_id"
                            "                      AND cst.ind_tipo_registro = 'DR'"
                            "                      AND cst.ind_facturable = 'S'"
                            "                      AND NOT EXISTS"
                            "                                 (SELECT 1"
                            "                                    FROM bok_extra EXT,"
                            "                                         discount_bond BON,"
                            "                                         campaign CAM"
                            "                                   WHERE CST.grec_seq_rec = EXT.grec_seq_rec"
                            "                                         AND CST.rres_seq_reserva = EXT.rres_seq_reserva"
                            "                                         AND CST.rext_ord_extra = EXT.ord_extra"
                            "                                         AND EXT.num_bono = BON.num_bono"
                            "                                         AND EXT.cod_interface = BON.cod_interface"
                            "                                         AND BON.cod_campana = CAM.cod_campana"
                            "                                         AND BON.cod_interface = CAM.cod_interface"
                            "                                         AND CAM.ind_rentabilidad = 'N')"
                            " group by operative_incoming, booking_id")

    df_aux.select("operative_incoming", "booking_id", "booking_currency").createOrReplaceTempView("aux")
    df_impuesto_canco.createOrReplaceTempView("canco")
    df_addcanco.createOrReplaceTempView("add_canco")

    df_r = manager.session.sql("select F.operative_incoming, F.booking_id, "
                                 "       round_ccy(NVL(SUM(impuesto_canco + cost_field), 0), F.booking_currency) "
                               "         tax_cost_transfer_pricing"
                                 " FROM aux F LEFT JOIN canco C "
                                 "   ON F.operative_incoming = C.seq_rec"
                                 "  AND F.booking_id = C.seq_reserva"
                                 "  LEFT JOIN add_canco A "
                                 "   ON F.operative_incoming = A.operative_incoming"
                                 "  AND F.booking_id = A.booking_id"
                                 " GROUP BY F.operative_incoming, F.booking_id, F.booking_currency")

    df_res = df_fields.join(df_r, ["operative_incoming", "booking_id"])\
        .select(df_fields.operative_incoming, df_fields.booking_id, df_fields.creation_date,
                df_fields.booking_currency, df_r.tax_cost_transfer_pricing)

    return df_res


def sub_tax_sales_transfer_pricing_eur(df_fields):
    df_fields = df_fields.withColumn('tax_sales_transfer_pricing_eur',
                                     udf_currency_exchange(df_fields.booking_currency,
                                                           func.lit(EUR),
                                                           df_fields.creation_date,
                                                           df_fields.tax_sales_transfer_pricing))

    df_fields = df_fields.na.fill({'tax_sales_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                                "tax_sales_transfer_pricing",
                                                                                "tax_sales_transfer_pricing_eur")

    return df_fields


def sub_tax_transfer_pricing_eur(df_fields):
    df_fields = df_fields.withColumn('tax_transfer_pricing_eur',
                                     udf_currency_exchange(df_fields.booking_currency,
                                                           func.lit(EUR),
                                                           df_fields.creation_date,
                                                           df_fields.transfer_pricing))

    df_fields = df_fields.na.fill({'tax_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                          "transfer_pricing",
                                                                          "tax_transfer_pricing_eur")

    return df_fields


def sub_tax_cost_transfer_pricing_eur(df_fields):
    df_fields = df_fields.withColumn('tax_cost_transfer_pricing_eur',
                                     udf_currency_exchange(df_fields.booking_currency,
                                                           func.lit(EUR),
                                                           df_fields.creation_date,
                                                           df_fields.tax_cost_transfer_pricing))

    df_fields = df_fields.na.fill({'tax_cost_transfer_pricing_eur': 0}).select("operative_incoming", "booking_id",
                                                                               "tax_cost_transfer_pricing",
                                                                               "tax_cost_transfer_pricing_eur")

    return df_fields


def join_all_fields(df_query, df_fields2, df_fields4, df_fields6):
    df_r1 = df_query.join(df_fields2, [df_query.operative_incoming == df_fields2.operative_incoming,
                                       df_query.booking_id == df_fields2.booking_id]) \
        .drop(df_fields2.operative_incoming).drop(df_fields2.booking_id)

    df_r2 = df_r1.join(df_fields4, [df_r1.operative_incoming == df_fields4.operative_incoming,
                                    df_r1.booking_id == df_fields4.booking_id]) \
        .drop(df_fields4.operative_incoming).drop(df_fields4.booking_id)

    df_r3 = df_r2.join(df_fields6, [df_r2.operative_incoming == df_fields6.operative_incoming,
                                    df_r2.booking_id == df_fields6.booking_id]) \
        .drop(df_fields6.operative_incoming).drop(df_fields6.booking_id)

    return df_r3


def change_date_type(dataframe):
    dataframe = dataframe.withColumn("creation_date", udf_date_to_datetime(dataframe.creation_date))
    dataframe = dataframe.withColumn("modification_date", udf_date_to_datetime(dataframe.modification_date))
    dataframe = dataframe.withColumn("cancellation_date", udf_date_to_datetime(dataframe.cancellation_date))
    dataframe = dataframe.withColumn("status_date", udf_date_to_datetime(dataframe.status_date))
    dataframe = dataframe.withColumn("booking_service_from", udf_date_to_datetime(dataframe.booking_service_from))
    dataframe = dataframe.withColumn("booking_service_to", udf_date_to_datetime(dataframe.booking_service_to))

    return dataframe


def remove_fields(dataframe):
    dataframe = dataframe.drop("Tax_Sales_Transfer_pricing") \
        .drop("Tax_Sales_Transfer_pricing_EUR") \
        .drop("Transfer_pricing") \
        .drop("tax_transfer_pricing_eur") \
        .drop("Tax_Cost_Transfer_pricing") \
        .drop("Tax_Cost_Transfer_pricing_EUR")

    return dataframe


