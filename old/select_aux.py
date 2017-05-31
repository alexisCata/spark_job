def select_direct_fields(manager):
    connection = psycopg2.connect(dbname=DATABASE, host=HOST,
                                  port=PORT, user=USER, password=PASS)

    cursor = connection.cursor()

    select = "with hotel as" \
             " (select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma," \
             "     row_number () over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum" \
             "   from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel from hbgdwc.dwc_bok_t_hotel_sale) hv" \
             "     inner join (select seq_hotel, izge_cod_destino from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel)" \
             "     inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id) di1 on (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG')" \
             " )," \
             " oth_con as" \
             " (select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, di2.sidi_cod_idioma, di2.nom_destino," \
             "     row_number () over (partition by o.seq_rec_other, o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum" \
             "   from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde as fec_desde_other from hbgdwc.dwc_bok_t_other) o" \
             "     inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other) c on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato AND c.ind_tipo_otro = o.ind_tipo_otro AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta)" \
             "     inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino from hbgdwc.dwc_itn_t_internet_destination_id) di2 on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG')" \
             " )," \
             " reventa as" \
             " (select seq_rec, seq_reserva, ind_status from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale" \
             " )," \
             " information as" \
             " (select i.seq_rec, i.seq_reserva, i.aplicacion," \
             "     row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum" \
             "   from hbgdwc.dwc_bok_t_booking_information i" \
             "   where i.tipo_op='A'" \
             " )," \
             " num_services as" \
             " (SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services" \
             "   FROM hbgdwc.dwc_bok_t_hotel_sale ri" \
             "   WHERE ri.fec_cancelacion is null" \
             "   group by ri.grec_seq_rec, ri.rres_seq_reserva" \
             " )," \
             " cabecera as" \
             " (select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion creation_date, grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, nro_ni," \
             "   gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, ind_facturable_res," \
             "   ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, ind_tippag" \
             "   from hbgdwc.dwc_bok_t_booking" \
             "   where fec_creacion BETWEEN '{}'::timestamp AND '{}'::timestamp" \
             "     and seq_reserva>0" \
             " )," \
            " ri as (SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion " \
             "                 FROM hbgdwc.dwc_bok_t_booking_information ri " \
             "                WHERE ri.tipo_op = 'A' " \
             "                group by ri.seq_rec, ri.seq_reserva) "\
             "select cabecera.grec_seq_rec grec_seq_rec, " \
             "       cabecera.seq_reserva seq_reserva, " \
             "       cabecera.gdiv_cod_divisa gdiv_cod_divisa, "\
             "       cabecera.creation_date fec_creacion, " \
             "       rf.semp_cod_emp semp_cod_emp_rf, " \
             "       cabecera.interface_id, " \
             "       re.semp_cod_emp operative_company, " \
             "       re.sofi_cod_ofi operative_office, " \
             "       re.des_receptivo operative_office_desc, " \
             "       cabecera.grec_seq_rec operative_incoming, " \
             "       cabecera.seq_reserva booking_id, " \
             "       cabecera.fint_cod_interface interface, " \
             "       rf.semp_cod_emp invoicing_company, " \
             "       rf.sofi_cod_ofi invoicing_office, " \
             "       rf.seq_rec invoicing_incoming, " \
             "       TRUNC (cabecera.creation_date) creation_date, " \
             "       cabecera.creation_date creation_ts, " \
             "       ri.min_fec_creacion first_booking_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date, " \
             "       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts, " \
             "       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date, " \
             "       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts, " \
             "       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking, " \
             "       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date, " \
             "       trunc(cabecera.fec_desde) booking_service_from, " \
             "       trunc(cabecera.fec_hasta) booking_service_to, " \
             "       t.seq_ttoo client_code, " \
             "       t.nom_corto_ttoo customer_name, " \
             "       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market, " \
             "       p.cod_iso source_market_iso, " \
             "       REPLACE(cabecera.nom_general,';','') holder, " \
             "       cabecera.ind_tipo_credito credit_type, " \
             "       cabecera.nro_ad num_adults, " \
             "       cabecera.nro_ni num_childrens, " \
             "       cabecera.gdep_cod_depart department_code, " \
             "       cabecera.rtre_cod_tipo_res booking_type, " \
             "       cabecera.ind_facturable_res invoicing_booking, " \
             "       cabecera.ind_facturable_adm invoicing_admin, " \
             "       NVL(cabecera.pct_comision,0) Client_commision_esp, " \
             "       NVL(cabecera.pct_rappel,0) client_override_esp, " \
             "       cabecera.ind_confirma confirmed_booking, " \
             "       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking, " \
             "       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency, " \
             "       NVL(cabecera.seq_ttoo_p,0) Partner_code, " \
             "       NVL(cabecera.cod_suc_p,0) Partner_brand, " \
             "       NVl(cabecera.seq_agencia_p,0) Partner_agency_code, " \
             "       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand, " \
             "       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming, " \
             "       NVL(cabecera.seq_res_expediente,0) booking_file_number, " \
             "       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model, " \
             "       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code, " \
             "       cabecera.gdiv_cod_divisa booking_currency, " \
             "       NVL(ttv.ttv, 0) TTV_booking_currency, " \
             "       NVL(ttv.ttv, 0)*NVL(tip.rate, 0) TTV_EUR_currency, " \
             "       NVL(tax_ttv.tax_ttv, 0) tax_ttv, " \
             "       NVL(tax_ttv.tax_ttv, 0)*NVL(tip.rate, 0) tax_ttv_eur, " \
             "       NVL(tax_ttv.tax_ttv_toms, 0) tax_ttv_toms, " \
             "       NVL(tax_ttv.tax_ttv_toms, 0)*NVL(tip.rate, 0) Tax_TTV_EUR_TOMS, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR, " \
             "       0 MISSING_CANCO_Transfer_pricing, " \
             "       0 missing_canco_tax_transfer_pricing_eur, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing, " \
             "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR, " \
             "       NVL(cli_comm.cli_comm, 0) Client_Commision, " \
             "       NVL(cli_comm.cli_comm, 0)*NVL(tip.rate, 0) Client_EUR_Commision, " \
             "       NVL(tax_cli_comm.Tax_Client_Com, 0) Tax_Client_commision, " \
             "       NVL(tax_cli_comm.Tax_Client_Com, 0)*NVL(tip.rate, 0) Tax_Client_EUR_commision, " \
             "       NVL(cli_rappel.cli_rappel, 0) client_rappel, " \
             "       NVL(cli_rappel.cli_rappel, 0)*NVL(tip.rate, 0) Client_EUR_rappel, " \
             "       NVL(tax_cli_rappel.tax_cli_rappel, 0) tax_client_rappel, " \
             "       NVL(tax_cli_rappel.tax_cli_rappel, 0)*NVL(tip.rate, 0) tax_Client_EUR_rappel, " \
             "       0 as missing_cost_booking_currency,   " \
             "       NVL(booking_cost.booking_cost, 0) cost_eur_currency, " \
             "       0 as MISSING_tax_cost, " \
             "       NVL(tax_cost.tax_cost, 0) tax_cost_EUR, " \
             "       0 as MISSING_tax_cost_TOMS, " \
             "       NVL(tax_cost.tax_cost_TOMS, 0) tax_cost_EUR_TOMS, " \
             "       inf.aplicacion Application, " \
             "       decode( " \
             "              decode(rev.ind_status, " \
             "                      'RR', 'R', " \
             "                      'CN', 'RL', " \
             "                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'), " \
             "                      rev.ind_status), " \
             "              'RS', 'Reventa', " \
             "              'C', 'Cancelada', " \
             "              'RL', 'Liberada', " \
             "              'R', 'Reventa') canrec_status, " \
             "       NVL(GSA_comm.GSA_Comm, 0) GSA_Commision, " \
             "       NVL(GSA_comm.GSA_Comm, 0)*NVL(tip.rate, 0) GSA_EUR_Commision, " \
             "       NVL(GSA_comm.Tax_GSA_Comm, 0) Tax_GSA_Commision, " \
             "       NVL(GSA_comm.Tax_GSA_Comm, 0)*NVL(tip.rate, 0) Tax_GSA_EUR_Commision, " \
             "       0 MISSING_Agency_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Agency_commision_hotel_pay,  " \
             "       NVL(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur, " \
             "       NVL(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur, " \
             "       0 MISSING_Fix_override_hotel_payment,  " \
             "       0 MISSING_Tax_Fix_override_hotel_pay,  " \
             "       NVL(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur, " \
             "       NVL(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur, " \
             "       0 MISSING_Var_override_hotel_payment,  " \
             "       0 MISSING_Tax_Var_override_hotel_pay,  " \
             "       NVL(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur, " \
             "       NVL(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR, " \
             "       0 MISSING_Hotel_commision_hotel_payment,  " \
             "       0 MISSING_Tax_Hotel_commision_Hotel_pay,  " \
             "       NVL(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur, " \
             "       NVL(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur, " \
             "       0 MISSING_Marketing_contribution,  " \
             "       0 MISSING_Tax_marketing_contribution,  " \
             "       NVL(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR, " \
             "       NVL(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR, " \
             "        " \
             "       0 MISSING_bank_expenses,  " \
             "       0 MISSING_Tax_Bank_expenses,  " \
             "       NVL(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR, " \
             "       NVL(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR, " \
             "       0 MISSING_Platform_Fee,  " \
             "       0 MISSING_Tax_Platform_fee,  " \
             "       NVL(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR, " \
             "       NVL(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR, " \
             "       0 MISSING_credit_card_fee,  " \
             "       0 missing_tax_credit_card_fee,   " \
             "       NVL(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR, " \
             "       NVL(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR, " \
             "       0 MISSING_Withholding,  " \
             "       0 MISSING_Tax_withholding,  " \
             "       NVL(withholding.Withholding_EUR, 0) Withholding_EUR, " \
             "       NVL(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR, " \
             "       0 MISSING_Local_Levy,  " \
             "       0 MISSING_Tax_Local_Levy,  " \
             "       NVL(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR, " \
             "       NVL(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR, " \
             "       0 MISSING_Partner_Third_commision,  " \
             "       0 MISSING_Tax_partner_third_commision,  " \
             "       NVL(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur, " \
             "       NVL(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur, " \
             "       NVL(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV " \
             "from " \
             " cabecera " \
             "inner join hbgdwc.dwc_mtd_t_ttoo t on  cabecera.gtto_seq_ttoo = t.seq_ttoo " \
             "inner join hbgdwc.dwc_gen_t_general_country p on (NVL(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais) " \
             "left join hbgdwc.dwc_mtd_t_receptive re on cabecera.grec_Seq_rec  = re.seq_rec  " \
             "left join hbgdwc.dwc_mtd_t_receptive rf on cabecera.seq_rec_hbeds = rf.seq_rec  " \
             "left join hbgdwc.dwc_itf_t_fc_interface i on cabecera.fint_cod_interface = i.cod_interface       " \
             "left join reventa rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva) " \
             "left join information inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1) " \
             "left join ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva " \
             "left join hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1) " \
             "left join oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1) " \
             "left join num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva) " \
             "left join hbgdwc.svd_david_ttv ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_ttv tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_client_com cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_client_com tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_client_rap cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_client_rap tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_cost booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_tax_cost tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_GSA_comm GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_age_comm_hot_pay age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_fix_over_hot_pay fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_var_over_hot_pay var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_hot_comm_hot_pay hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_marketing_contrib marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_bank_exp bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_platform_fee platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_credit_card_fee credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_withholding withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_local_levy local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.svd_david_partner_third_comm partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id " \
             "left join hbgdwc.david_tasas_cambio_flc tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency ".format(
        sys.argv[1], sys.argv[2])

    # select = "select cabecera.grec_seq_rec grec_seq_rec, " \
    #          "       cabecera.seq_reserva seq_reserva, " \
    #          "       cabecera.gdiv_cod_divisa gdiv_cod_divisa, "\
    #          "       cabecera.fec_creacion fec_creacion, " \
    #          "       rf.semp_cod_emp semp_cod_emp_rf, " \
    #          "       cabecera.interface_id, " \
    #          "       re.semp_cod_emp operative_company, " \
    #          "       re.sofi_cod_ofi operative_office, " \
    #          "       re.des_receptivo operative_office_desc, " \
    #          "       cabecera.grec_seq_rec operative_incoming, " \
    #          "       cabecera.seq_reserva booking_id, " \
    #          "       cabecera.fint_cod_interface interface, " \
    #          "       rf.semp_cod_emp invoicing_company, " \
    #          "       rf.sofi_cod_ofi invoicing_office, " \
    #          "       rf.seq_rec invoicing_incoming, " \
    #          "       TRUNC (cabecera.creation_date) creation_date, " \
    #          "       cabecera.creation_date creation_ts, " \
    #          "       ri.min_fec_creacion first_booking_ts, " \
    #          "       NVL(TO_CHAR(TRUNC (cabecera.fec_modifica),'yyyy-mm-dd'),'') modification_date, " \
    #          "       NVL(TO_CHAR(cabecera.fec_modifica,'yyyy-mm-dd hh24:mi:ss'),'') modification_ts, " \
    #          "       NVL(TO_CHAR(TRUNC (cabecera.fec_cancelacion),'yyyy-mm-dd'),'') cancellation_date, " \
    #          "       NVL(TO_CHAR(cabecera.fec_cancelacion,'yyyy-mm-dd hh24:mi:ss'),'') cancellation_ts, " \
    #          "       decode(cabecera.fec_cancelacion, null, 'N', 'S') cancelled_booking, " \
    #          "       TRUNC(GREATEST(cabecera.creation_date, cabecera.fec_modifica, cabecera.fec_cancelacion)) status_date, " \
    #          "       trunc(cabecera.fec_desde) booking_service_from, " \
    #          "       trunc(cabecera.fec_hasta) booking_service_to, " \
    #          "       t.seq_ttoo client_code, " \
    #          "       t.nom_corto_ttoo customer_name, " \
    #          "       NVL (cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) source_market, " \
    #          "       p.cod_iso source_market_iso, " \
    #          "       REPLACE(cabecera.nom_general,';','') holder, " \
    #          "       cabecera.ind_tipo_credito credit_type, " \
    #          "       cabecera.nro_ad num_adults, " \
    #          "       cabecera.nro_ni num_childrens, " \
    #          "       cabecera.gdep_cod_depart department_code, " \
    #          "       cabecera.rtre_cod_tipo_res booking_type, " \
    #          "       cabecera.ind_facturable_res invoicing_booking, " \
    #          "       cabecera.ind_facturable_adm invoicing_admin, " \
    #          "       NVL(cabecera.pct_comision,0) Client_commision_esp, " \
    #          "       NVL(cabecera.pct_rappel,0) client_override_esp, " \
    #          "       cabecera.ind_confirma confirmed_booking, " \
    #          "       decode (i.partner_ttoo, null, 'N', 'S') Partner_booking, " \
    #          "       NVL(cabecera.cod_divisa_p,'') Partner_booking_currency, " \
    #          "       NVL(cabecera.seq_ttoo_p,0) Partner_code, " \
    #          "       NVL(cabecera.cod_suc_p,0) Partner_brand, " \
    #          "       NVl(cabecera.seq_agencia_p,0) Partner_agency_code, " \
    #          "       NVL(cabecera.seq_sucursal_p,0) Partner_agency_brand, " \
    #          "       NVL(cabecera.seq_rec_expediente,0) Booking_file_incoming, " \
    #          "       NVL(cabecera.seq_res_expediente,0) booking_file_number, " \
    #          "       decode (cabecera.ind_tippag, null, 'Merchant', 'Pago en hotel') Accomodation_model, " \
    #          "       NVL ( hotel.izge_cod_destino || '-' || hotel.nom_destino, oth_con.cod_destino || '-' ||oth_con.nom_destino, 'NO_DESTINATION_CODE'  ) Destination_code, " \
    #          "       cabecera.gdiv_cod_divisa booking_currency, " \
    #          "       NVL(ttv.ttv, 0) TTV_booking_currency, " \
    #          "       NVL(ttv.ttv, 0)*NVL(tip.rate, 0) TTV_EUR_currency, " \
    #          "       NVL(tax_ttv.tax_ttv, 0) tax_ttv, " \
    #          "       NVL(tax_ttv.tax_ttv, 0)*NVL(tip.rate, 0) tax_ttv_eur, " \
    #          "       NVL(tax_ttv.tax_ttv_toms, 0) tax_ttv_toms, " \
    #          "       NVL(tax_ttv.tax_ttv_toms, 0)*NVL(tip.rate, 0) Tax_TTV_EUR_TOMS, " \
    #          "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing, " \
    #          "       0 MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR, " \
    #          "       0 MISSING_CANCO_Transfer_pricing, " \
    #          "       0 missing_canco_tax_transfer_pricing_eur, " \
    #          "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing, " \
    #          "       0 MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR, " \
    #          "       NVL(cli_comm.cli_comm, 0) Client_Commision, " \
    #          "       NVL(cli_comm.cli_comm, 0)*NVL(tip.rate, 0) Client_EUR_Commision, " \
    #          "       NVL(tax_cli_comm.Tax_Client_Com, 0) Tax_Client_commision, " \
    #          "       NVL(tax_cli_comm.Tax_Client_Com, 0)*NVL(tip.rate, 0) Tax_Client_EUR_commision, " \
    #          "       NVL(cli_rappel.cli_rappel, 0) client_rappel, " \
    #          "       NVL(cli_rappel.cli_rappel, 0)*NVL(tip.rate, 0) Client_EUR_rappel, " \
    #          "       NVL(tax_cli_rappel.tax_cli_rappel, 0) tax_client_rappel, " \
    #          "       NVL(tax_cli_rappel.tax_cli_rappel, 0)*NVL(tip.rate, 0) tax_Client_EUR_rappel, " \
    #          "       0 as missing_cost_booking_currency,   " \
    #          "       NVL(booking_cost.booking_cost, 0) cost_eur_currency, " \
    #          "       0 as MISSING_tax_cost, " \
    #          "       NVL(tax_cost.tax_cost, 0) tax_cost_EUR, " \
    #          "       0 as MISSING_tax_cost_TOMS, " \
    #          "       NVL(tax_cost.tax_cost_TOMS, 0) tax_cost_EUR_TOMS, " \
    #          "       inf.aplicacion Application, " \
    #          "       decode( " \
    #          "              decode(rev.ind_status, " \
    #          "                      'RR', 'R', " \
    #          "                      'CN', 'RL', " \
    #          "                      'RL', decode(cabecera.fec_cancelacion, null, 'RS', 'C'), " \
    #          "                      rev.ind_status), " \
    #          "              'RS', 'Reventa', " \
    #          "              'C', 'Cancelada', " \
    #          "              'RL', 'Liberada', " \
    #          "              'R', 'Reventa') canrec_status, " \
    #          "       NVL(GSA_comm.GSA_Comm, 0) GSA_Commision, " \
    #          "       NVL(GSA_comm.GSA_Comm, 0)*NVL(tip.rate, 0) GSA_EUR_Commision, " \
    #          "       NVL(GSA_comm.Tax_GSA_Comm, 0) Tax_GSA_Commision, " \
    #          "       NVL(GSA_comm.Tax_GSA_Comm, 0)*NVL(tip.rate, 0) Tax_GSA_EUR_Commision, " \
    #          "       0 MISSING_Agency_commision_hotel_payment,  " \
    #          "       0 MISSING_Tax_Agency_commision_hotel_pay,  " \
    #          "       NVL(age_com_hot_pay.Agency_commision_hotel_payment_EUR, 0) agency_comm_hotel_pay_eur, " \
    #          "       NVL(age_com_hot_pay.Tax_Agency_commision_hotel_pay_EUR, 0) tax_agency_comm_hotel_pay_eur, " \
    #          "       0 MISSING_Fix_override_hotel_payment,  " \
    #          "       0 MISSING_Tax_Fix_override_hotel_pay,  " \
    #          "       NVL(fix_over_hot_pay.Fix_override_hotel_payment_EUR, 0) fix_override_hotel_pay_eur, " \
    #          "       NVL(fix_over_hot_pay.Tax_Fix_override_hotel_pay_EUR, 0) tax_fix_overr_hotel_pay_eur, " \
    #          "       0 MISSING_Var_override_hotel_payment,  " \
    #          "       0 MISSING_Tax_Var_override_hotel_pay,  " \
    #          "       NVL(var_over_hot_pay.Var_override_hotel_payment_EUR, 0) var_override_hotel_pay_eur, " \
    #          "       NVL(var_over_hot_pay.Tax_Var_override_hotel_pay_EUR, 0) Tax_Var_override_hotel_pay_EUR, " \
    #          "       0 MISSING_Hotel_commision_hotel_payment,  " \
    #          "       0 MISSING_Tax_Hotel_commision_Hotel_pay,  " \
    #          "       NVL(hot_comm_hot_pay.Hotel_commision_hotel_payment_EUR, 0) hotel_commision_hotel_pay_eur, " \
    #          "       NVL(hot_comm_hot_pay.Tax_Hotel_commision_Hotel_pay_EUR, 0) tax_hotel_comm_hotel_pay_eur, " \
    #          "       0 MISSING_Marketing_contribution,  " \
    #          "       0 MISSING_Tax_marketing_contribution,  " \
    #          "       NVL(marketing_contrib.Marketing_contribution_EUR, 0) Marketing_contribution_EUR, " \
    #          "       NVL(marketing_contrib.Tax_marketing_contribution_EUR, 0) Tax_marketing_contribution_EUR, " \
    #          "        " \
    #          "       0 MISSING_bank_expenses,  " \
    #          "       0 MISSING_Tax_Bank_expenses,  " \
    #          "       NVL(bank_exp.bank_expenses_EUR, 0) bank_expenses_EUR, " \
    #          "       NVL(bank_exp.Tax_Bank_expenses_EUR, 0) Tax_Bank_expenses_EUR, " \
    #          "       0 MISSING_Platform_Fee,  " \
    #          "       0 MISSING_Tax_Platform_fee,  " \
    #          "       NVL(platform_fee.Platform_Fee_EUR, 0) Platform_Fee_EUR, " \
    #          "       NVL(platform_fee.Tax_Platform_fee_EUR, 0) Tax_Platform_fee_EUR, " \
    #          "       0 MISSING_credit_card_fee,  " \
    #          "       0 missing_tax_credit_card_fee,   " \
    #          "       NVL(credit_card_fee.credit_card_fee_EUR, 0) credit_card_fee_EUR, " \
    #          "       NVL(credit_card_fee.Tax_credit_card_fee_EUR, 0) Tax_credit_card_fee_EUR, " \
    #          "       0 MISSING_Withholding,  " \
    #          "       0 MISSING_Tax_withholding,  " \
    #          "       NVL(withholding.Withholding_EUR, 0) Withholding_EUR, " \
    #          "       NVL(withholding.Tax_withholding_EUR, 0) Tax_withholding_EUR, " \
    #          "       0 MISSING_Local_Levy,  " \
    #          "       0 MISSING_Tax_Local_Levy,  " \
    #          "       NVL(local_levy.Local_Levy_EUR, 0) Local_Levy_EUR, " \
    #          "       NVL(local_levy.Tax_Local_Levy_EUR, 0) Tax_Local_Levy_EUR, " \
    #          "       0 MISSING_Partner_Third_commision,  " \
    #          "       0 MISSING_Tax_partner_third_commision,  " \
    #          "       NVL(partner_third_comm.Partner_Third_commision_EUR, 0) partner_third_comm_eur, " \
    #          "       NVL(partner_third_comm.Tax_partner_third_commision_EUR, 0) tax_partner_third_comm_eur, " \
    #          "       NVL(num_services.act_services, 0) NUMBER_ACTIVE_ACC_SERV " \
    #          "from " \
    #          "(select grec_Seq_rec, seq_reserva, gdiv_cod_divisa booking_currency, fec_creacion creation_date, grec_seq_rec || '-' || seq_reserva interface_id, gtto_seq_ttoo, cod_pais_cliente, nro_ad, nro_ni, " \
    #          "  gdiv_cod_divisa, fec_modifica, fec_cancelacion, fec_desde, fint_cod_interface, seq_rec_hbeds, fec_hasta, nom_general, ind_tipo_credito, gdep_cod_depart, rtre_cod_tipo_res, ind_facturable_res, " \
    #          "  ind_facturable_adm, pct_comision, pct_rappel, ind_confirma, cod_divisa_p, seq_ttoo_p, cod_suc_p, seq_agencia_p, seq_sucursal_p, seq_rec_expediente, seq_res_expediente, ind_tippag, fec_creacion  " \
    #          "  from hbgdwc.dwc_bok_t_booking " \
    #          "  where fec_creacion BETWEEN '{}'::timestamp AND '{}'::timestamp " \
    #          "    and seq_reserva>0 " \
    #          "    ) cabecera " \
    #          "inner join hbgdwc.dwc_mtd_t_ttoo t on  cabecera.gtto_seq_ttoo = t.seq_ttoo " \
    #          "inner join hbgdwc.dwc_gen_t_general_country p on (NVL(cabecera.cod_pais_cliente, t.gpai_cod_pais_mercado) = p.cod_pais) " \
    #          "left join hbgdwc.dwc_mtd_t_receptive re on cabecera.grec_Seq_rec  = re.seq_rec  " \
    #          "left join hbgdwc.dwc_mtd_t_receptive rf on cabecera.seq_rec_hbeds = rf.seq_rec  " \
    #          "left join hbgdwc.dwc_itf_t_fc_interface i on cabecera.fint_cod_interface = i.cod_interface       " \
    #          "left join (select seq_rec, seq_reserva, ind_status from hbgdwc.dwc_cry_t_cancellation_recovery_res_resale " \
    #          ") rev on (rev.seq_rec=cabecera.grec_seq_rec and rev.seq_reserva=cabecera.seq_reserva) " \
    #          "left join (select i.seq_rec, i.seq_reserva, i.aplicacion, " \
    #          "    row_number () over (partition by i.seq_rec, i.seq_reserva) as rownum " \
    #          "  from hbgdwc.dwc_bok_t_booking_information i " \
    #          "  where i.tipo_op='A' " \
    #          ") inf on (inf.seq_rec=cabecera.grec_seq_rec and inf.seq_reserva=cabecera.seq_reserva and inf.rownum=1) " \
    #          "left join (SELECT ri.seq_rec, ri.seq_reserva, MIN(ri.fec_creacion) min_fec_creacion " \
    #          "                 FROM hbgdwc.dwc_bok_t_booking_information ri " \
    #          "                WHERE ri.tipo_op = 'A' " \
    #          "                group by ri.seq_rec, ri.seq_reserva) ri on ri.seq_rec = cabecera.grec_seq_rec AND ri.seq_reserva = cabecera.seq_reserva " \
    #          "left join  " \
    #          "(select hv.grec_Seq_rec, hv.rres_seq_reserva, hv.ghor_seq_hotel, h.seq_hotel, h.izge_cod_destino, di1.ides_cod_destino, di1.nom_destino, di1.sidi_cod_idioma, " \
    #          "    row_number () over (partition by hv.grec_Seq_rec, hv.rres_seq_reserva order by grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel) as rownum " \
    #          "  from (select grec_Seq_rec, rres_seq_reserva, ghor_seq_hotel from hbgdwc.dwc_bok_t_hotel_sale) hv " \
    #          "    inner join (select seq_hotel, izge_cod_destino from hbgdwc.dwc_mtd_t_hotel) h on (h.seq_hotel = hv.ghor_seq_hotel) " \
    #          "    inner join (select ides_cod_destino, nom_destino, sidi_cod_idioma from hbgdwc.dwc_itn_t_internet_destination_id) di1 on (di1.ides_cod_destino = h.izge_cod_destino AND di1.sidi_cod_idioma  = 'ENG') " \
    #          ") hotel on (hotel.grec_Seq_rec = cabecera.grec_seq_rec AND hotel.rres_seq_reserva = cabecera.seq_reserva and hotel.rownum=1) " \
    #          "left join  " \
    #          "(select o.seq_rec_other, o.seq_reserva, o.nom_contrato, o.ind_tipo_otro, o.fec_desde_other, c.seq_rec, c.nom_contrato, c.cod_destino, c.ind_tipo_otro, c.fec_desde, c.fec_hasta, di2.ides_cod_Destino, di2.sidi_cod_idioma, di2.nom_destino, " \
    #          "    row_number () over (partition by o.seq_rec_other, o.seq_reserva order by o.seq_rec_other, o.seq_reserva, o.nom_contrato) as rownum " \
    #          "  from (select seq_rec as seq_rec_other, seq_reserva, nom_contrato, ind_tipo_otro, fec_desde as fec_desde_other from hbgdwc.dwc_bok_t_other) o " \
    #          "    inner join (select seq_rec, nom_contrato, cod_destino, ind_tipo_otro, fec_desde, fec_hasta from hbgdwc.dwc_con_t_contract_other) c on (c.seq_rec = o.seq_rec_other AND c.nom_contrato = o.nom_contrato AND c.ind_tipo_otro = o.ind_tipo_otro AND o.fec_desde_other BETWEEN c.fec_desde AND c.fec_hasta) " \
    #          "    inner join (select ides_cod_Destino, sidi_cod_idioma, nom_destino from hbgdwc.dwc_itn_t_internet_destination_id) di2 on (di2.ides_cod_Destino = c.cod_destino AND di2.sidi_cod_idioma  = 'ENG') " \
    #          ") oth_con on (oth_con.seq_rec_other = cabecera.grec_seq_rec AND oth_con.seq_reserva = cabecera.seq_reserva and oth_con.rownum=1) " \
    #          "left join (SELECT ri.grec_seq_rec, ri.rres_seq_reserva, COUNT(1) act_services " \
    #          "  FROM hbgdwc.dwc_bok_t_hotel_sale ri " \
    #          "  WHERE ri.fec_cancelacion is null " \
    #          "  group by ri.grec_seq_rec, ri.rres_seq_reserva " \
    #          ") num_services on (num_services.grec_Seq_rec = cabecera.grec_seq_rec AND num_services.rres_seq_reserva = cabecera.seq_reserva) " \
    #          "left join hbgdwc.svd_david_ttv ttv on ttv.grec_seq_rec||'-'||ttv.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_ttv tax_ttv on tax_ttv.grec_seq_rec||'-'||tax_ttv.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_client_com cli_comm on cli_comm.grec_seq_rec||'-'||cli_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_client_com tax_cli_comm on tax_cli_comm.grec_seq_rec||'-'||tax_cli_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_client_rap cli_rappel on cli_rappel.grec_seq_rec||'-'||cli_rappel.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_client_rap tax_cli_rappel on tax_cli_rappel.grec_seq_rec||'-'||tax_cli_rappel.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_cost booking_cost on booking_cost.grec_seq_rec||'-'||booking_cost.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_tax_cost tax_cost on tax_cost.grec_seq_rec||'-'||tax_cost.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_GSA_comm GSA_comm on GSA_comm.seq_rec||'-'||GSA_comm.seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_age_comm_hot_pay age_com_hot_pay on age_com_hot_pay.grec_seq_rec||'-'||age_com_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_fix_over_hot_pay fix_over_hot_pay on fix_over_hot_pay.grec_seq_rec||'-'||fix_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_var_over_hot_pay var_over_hot_pay on var_over_hot_pay.grec_seq_rec||'-'||var_over_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_hot_comm_hot_pay hot_comm_hot_pay on hot_comm_hot_pay.grec_seq_rec||'-'||hot_comm_hot_pay.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_marketing_contrib marketing_contrib on marketing_contrib.grec_seq_rec||'-'||marketing_contrib.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_bank_exp bank_exp on bank_exp.grec_seq_rec||'-'||bank_exp.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_platform_fee platform_fee on platform_fee.grec_seq_rec||'-'||platform_fee.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_credit_card_fee credit_card_fee on credit_card_fee.grec_seq_rec||'-'||credit_card_fee.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_withholding withholding on withholding.grec_seq_rec||'-'||withholding.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_local_levy local_levy on local_levy.grec_seq_rec||'-'||local_levy.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.svd_david_partner_third_comm partner_third_comm on partner_third_comm.grec_seq_rec||'-'||partner_third_comm.rres_seq_reserva=cabecera.interface_id " \
    #          "left join hbgdwc.david_tasas_cambio_flc tip on trunc(cabecera.creation_date)=tip.date and cabecera.booking_currency=tip.currency ".format(
    #     sys.argv[1], sys.argv[2])

    cursor.execute(select)

    results = cursor.fetchall()

    headers = ["grec_seq_rec", "seq_reserva", "gdiv_cod_divisa", "fec_creacion", "semp_cod_emp_rf",
               "interface_id", "operative_company", "operative_office", "operative_office_desc", "operative_incoming",
               "booking_id", "interface", "invoicing_company", "invoicing_office", "invoicing_incoming",
               "creation_date", "creation_ts", "first_booking_ts", "modification_date", "modification_ts",
               "cancellation_date", "cancellation_ts", "cancelled_booking", "status_date", "booking_service_from",
               "booking_service_to", "client_code", "customer_name", "source_market", "source_market_iso", "holder",
               "credit_type", "num_adults", "num_childrens", "department_code", "booking_type", "invoicing_booking",
               "invoicing_admin", "client_commision_esp", "client_override_esp", "confirmed_booking", "Partner_booking",
               "Partner_booking_currency", "Partner_code", "Partner_brand", "Partner_agency_code",
               "Partner_agency_brand", "Booking_file_incoming", "booking_file_number", "Accomodation_model",
               "Destination_code", "booking_currency", "TTV_booking_currency", "TTV_EUR_currency", "tax_ttv",
               "tax_ttv_eur", "tax_ttv_toms", "Tax_TTV_EUR_TOMS", "MISSING_CANCO_Tax_Sales_Transfer_pricing",
               "MISSING_CANCO_Tax_Sales_Transfer_pricing_EUR", "MISSING_CANCO_Transfer_pricing",
               "missing_canco_tax_transfer_pricing_eur", "MISSING_CANCO_Tax_Cost_Transfer_pricing",
               "MISSING_CANCO_Tax_Cost_Transfer_pricing_EUR", "Client_Commision", "Client_EUR_Commision",
               "Tax_Client_commision", "Tax_Client_EUR_commision", "client_rappel", "Client_EUR_rappel",
               "tax_client_rappel", "tax_Client_EUR_rappel", "missing_cost_booking_currency", "cost_eur_currency",
               "MISSING_tax_cost", "tax_cost_EUR", "MISSING_tax_cost_TOMS", "tax_cost_EUR_TOMS", "Application",
               "canrec_status", "GSA_Commision", "GSA_EUR_Commision", "Tax_GSA_Commision", "Tax_GSA_EUR_Commision",
               "MISSING_Agency_commision_hotel_payment", "MISSING_Tax_Agency_commision_hotel_pay",
               "agency_comm_hotel_pay_eur", "tax_agency_comm_hotel_pay_eur", "MISSING_Fix_override_hotel_payment",
               "MISSING_Tax_Fix_override_hotel_pay", "fix_override_hotel_pay_eur", "tax_fix_overr_hotel_pay_eur",
               "MISSING_Var_override_hotel_payment", "MISSING_Tax_Var_override_hotel_pay",
               "var_override_hotel_pay_eur", "Tax_Var_override_hotel_pay_EUR", "MISSING_Hotel_commision_hotel_payment",
               "MISSING_Tax_Hotel_commision_Hotel_pay", "hotel_commision_hotel_pay_eur", "tax_hotel_comm_hotel_pay_eur",
               "MISSING_Marketing_contribution", "MISSING_Tax_marketing_contribution", "Marketing_contribution_EUR",
               "Tax_marketing_contribution_EUR", "MISSING_bank_expenses", "MISSING_Tax_Bank_expenses",
               "bank_expenses_EUR", "Tax_Bank_expenses_EUR", "MISSING_Platform_Fee", "MISSING_Tax_Platform_fee",
               "Platform_Fee_EUR", "Tax_Platform_fee_EUR", "MISSING_credit_card_fee", "missing_tax_credit_card_fee",
               "credit_card_fee_EUR", "Tax_credit_card_fee_EUR", "MISSING_Withholding", "MISSING_Tax_withholding",
               "Withholding_EUR", "Tax_withholding_EUR", "MISSING_Local_Levy", "MISSING_Tax_Local_Levy",
               "Local_Levy_EUR", "Tax_Local_Levy_EUR", "MISSING_Partner_Third_commision",
               "MISSING_Tax_partner_third_commision", "partner_third_comm_eur", "tax_partner_third_comm_eur",
               "NUMBER_ACTIVE_ACC_SERV"]

    dataframe = manager.session.createDataFrame(results, headers)

    return dataframe


def direct_fields(manager):
    df_booking = manager.get_dataframe(tables['dwc_bok_t_booking'].format(sys.argv[1], sys.argv[2],
                                                                          sys.argv[1], sys.argv[2]))
    df_ttoo = manager.get_dataframe(tables['dwc_mtd_t_ttoo'])
    df_receptive = manager.get_dataframe(tables['dwc_mtd_t_receptive'])
    df_receptivef = manager.get_dataframe(tables['dwc_mtd_t_receptive_f'])
    df_interface = manager.get_dataframe(tables['dwc_itf_t_fc_interface'])
    df_country = manager.get_dataframe(tables['dwc_gen_t_general_country'])

    df_aux = df_booking.join(df_ttoo, df_booking.gtto_seq_ttoo == df_ttoo.seq_ttoo)

    # CHECK nanvl and try to avoid withColumn
    df_booking_aux = df_aux.withColumn('cod_pais_cliente',
                                       func.when(df_aux.cod_pais_cliente.isNotNull(), df_aux.cod_pais_cliente).
                                       otherwise(df_aux.gpai_cod_pais_mercado))

    df_res = df_booking_aux.join(df_receptive, df_booking_aux.grec_seq_rec == df_receptive.seq_rec_re). \
        join(df_receptivef, df_booking_aux.seq_rec_hbeds == df_receptivef.seq_rec_rf). \
        join(df_interface, df_booking_aux.fint_cod_interface == df_interface.cod_interface). \
        join(df_country, df_booking_aux.cod_pais_cliente == df_country.cod_pais)

    df_res_aux = df_res.selectExpr("CASE WHEN fec_cancelacion is NULL THEN 'N' ELSE 'S' END AS cancelled_booking",
                                   "CASE WHEN partner_ttoo is NULL THEN 'N' ELSE 'S' END AS Partner_booking",
                                   "CASE WHEN ind_tippag is NULL THEN 'Merchant' ELSE 'Pago en hotel' END AS Accomodation_model",
                                   "*")
    df_direct_fields = df_res_aux. \
        select(  # fields for be used with the subselects
        df_res_aux.grec_seq_rec,
        df_res_aux.seq_reserva,
        df_res_aux.semp_cod_emp_re,
        df_res_aux.semp_cod_emp_rf,
        df_res_aux.fec_desde,
        df_res_aux.fec_creacion,
        df_res_aux.gdiv_cod_divisa,
        df_res_aux.ind_tippag,
        df_res_aux.ind_fec_cam_div,
        # result fields
        func.concat(df_res_aux.grec_seq_rec, func.lit('-'), df_res_aux.seq_reserva).alias('interface_id'),
        df_res_aux.semp_cod_emp_re.alias('operative_company'),
        df_res_aux.sofi_cod_ofi_re.alias('operative_office'),
        df_res_aux.des_receptivo.alias('operative_office_desc'),
        df_res_aux.grec_seq_rec.alias('operative_incoming'),
        df_res_aux.seq_reserva.alias('booking_id'),
        df_res_aux.fint_cod_interface.alias('interface'),
        df_res_aux.semp_cod_emp_rf.alias('invoicing_company'),
        df_res_aux.sofi_cod_ofi_rf.alias('invoicing_office'),
        df_res_aux.seq_rec_rf.alias('invoicing_incoming'),
        func.date_format(df_res_aux.fec_creacion, 'yyyy-MM-dd').alias('Creation_date'),
        df_res_aux.fec_creacion.alias('Creation_ts'),
        func.date_format(df_res_aux.fec_modifica, 'yyyy-MM-dd').alias('modification_date'),
        df_res_aux.fec_modifica.alias('modification_ts'),
        func.date_format(df_res_aux.fec_cancelacion, 'yyyy-MM-dd').alias('cancellation_date'),
        df_res_aux.fec_cancelacion.alias('cancellation_ts'),
        # DECODE
        df_res_aux.cancelled_booking,
        func.date_format(func.greatest(df_res_aux.fec_creacion, df_res_aux.fec_modifica, df_res_aux.fec_cancelacion),
                         'yyyy-MM-dd').alias('status_date'),
        df_res_aux.fec_desde.alias('booking_service_from'),
        df_res_aux.fec_hasta.alias('booking_service_to'),
        df_res_aux.seq_ttoo.alias('client_code'),
        df_res_aux.nom_corto_ttoo.alias('costumer_name'),
        df_res_aux.cod_pais_cliente.alias('Source_market'),
        df_res_aux.cod_iso.alias('source_market_iso'),
        func.regexp_replace(df_res_aux.nom_general, ';', '').alias('Holder'),
        df_res_aux.nro_ad.alias('num_adults'),
        df_res_aux.nro_ni.alias('num_childrens'),
        df_res_aux.gdep_cod_depart.alias('Department_code'),
        df_res_aux.rtre_cod_tipo_res.alias('Booking_type'),
        df_res_aux.ind_facturable_res.alias('Invoicing_booking'),
        df_res_aux.ind_facturable_adm.alias('Invoicing_admin'),
        df_res_aux.pct_comision.alias('Client_commision_esp'),
        df_res_aux.pct_rappel.alias('client_override_esp'),
        df_res_aux.ind_confirma.alias('confirmed_booking'),
        # DECODE
        df_res_aux.Partner_booking,
        df_res_aux.cod_divisa_p.alias('Partner_booking_currency'),
        df_res_aux.seq_ttoo_p.alias('Partner_code'),
        df_res_aux.cod_suc_p.alias('Partner_brand'),
        df_res_aux.seq_agencia_p.alias('Partner_agency_code'),
        df_res_aux.seq_sucursal_p.alias('Partner_agency_brand'),
        df_res_aux.seq_rec_expediente.alias('Booking_file_incoming'),
        df_res_aux.seq_res_expediente.alias('booking_file_number'),
        # DECODE
        df_res_aux.Accomodation_model,
        df_res_aux.gdiv_cod_divisa.alias('Booking_currency'))

    return df_direct_fields

