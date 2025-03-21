import java.sql.{Date, Timestamp}

object Models {

  case class EsdlTransaction(
                              txn_id:String,
                              card_number:String,
                              account_number:String,
                              holding_branch_key:String,
                              account_key:String,
                              source_system_cd:String,
                              channel_cd:String,
                              source_transaction_id:String,
                              execution_local_date_time:Timestamp,
                              posting_Date:Timestamp,
                              cr_dr_code:String,
                              cash_code:String,
                              msg_type_code:String,
                              acct_curr_cd:String,
                              acct_curr_amount: BigDecimal,
                              orig_curr_cd:String,
                              orig_curr_amount:String,
                              cad_equivalent_amt:String,
                              orig_curr_cash_amount:String,
                              case_Cad_equivalent_amt:String,
                              acct_to_cad_rate:String,
                              txn_to_acct_rate:String,
                              txn_to_cad_rate:String,
                              txn_curr_rate_ind:String,
                              ecif_compisite_key:String,
                              ip_address:String,
                              orph_ind:String,
                              orig_process_date:String,
                              loaded_to_cerebro:String,
                              loaded_to_hunter:String,
                              opp_account_number:String,
                              opp_branch_key:String,
                              rules_cash_cad_equivalent_amt:String,
                              operation_type:String,
                              txn_status_code:String,
                              txn_response_cd:String,
                              rules_cad_equivalent_amt:String,
                              emit_transfer_cd:String,
                              emit_recipient_name:String,
                              recipient_sms:String,
                              sender_email:String,
                              processing_Date:Date,
                              recipient_email:String,
                              instr_agent_id:String,
                              instrg_agent_clearing_system:String,
                              txn_status:String,
                              txn_type:String,
                              sndr_agt_name:String,
                              utc_txn_date:Timestamp,
                              utc_txn_time:Timestamp,
                              cust1_org_legal_name:String,
                              cust2_org_legal_name:String,
                              cust1_bank_name:String,
                              cust2_bank_name:String,
                              currency_conversion_rate:String,
                              user_id:String,
                              user_device_type:String,
                              user_session_date_time:String,
                              transaction_memo_line_1:String,
                              client_ip_addr:String,
                              debtor_id:String,
                              addl_field_10:String,
                              fx_rate_exchange_rate:String,
                              addl_field_7:String,
                              addl_field_8:String,
                              addl_field_9:String,
                              row_update_date: Timestamp,
                              cash_cad_equivalent_amt: String,
                              ecif_composite_key: String,
                              emt_transfer_id: String,
                              emt_recipient_id: String,
                              organization_unit_cd: String,
                              opp_organization_unit_cd: String,
                              fx_tran_exchange_rate: String
                            )

  case class StgCertPayAmlReport(
                                  cr_dr_ind:String,
                                  account_servicer_reference:String,
                                  src_txn_id:String,
                                  debtor_Account:String,
                                  party_key:String,
                                  account_number:String,
                                  holding_branch_key:String,
                                  product_type_code:String,
                                  relation_type_cd:String,
                                  account_key:String,
                                  acceptance_date_time:Timestamp,
                                  msg_type_Code:String,
                                  currency:String,
                                  amount: BigDecimal,
                                  creditor_Account:String,
                                  payment_status:String,
                                  interac_ref_num:String,
                                  creditor_name:String,
                                  creditor_mobile_no:String,
                                  debtor_email:String,
                                  creditor_email_phone:String,
                                  instructed_agent_fi:String,
                                  instructing_agent_fi:String,
                                  transaction_type:String,
                                  transaction_type_desc:String,
                                  debtor_name:String,
                                  debtor_legal_name:String,
                                  creditor_legal_name:String,
                                  creditor_memo:String,
                                  creditor_ip_address:String,
                                  creditor_id:String,
                                  debtor_id:String,
                                  fraud_check_Action:String,
                                  transaction_id:String


                                )

  case class EsdlPartyProd(
                            party_key:String,
                            account_number:String,
                            holding_branch_key:String,
                            product_type_code:String,
                            relation_type_Cd:String,
                            amount_key: String,
                            ecif_composite_key:String
                          )

  case class EsdlAccOpenDate(
                              ecif_composite_key:String,
                              curr_plc_acct_num:String,
                              holding_branch_key_source:String,
                              product_type_code: String
                            )

  case class EsdlRef(
                      typ: String,
                      source_id:String,
                      source_cd:String,
                      target_cd:String,
                      effective_from:String,
                      effective_to:String,
                      target_json: String,
                      source_json: String

                    )

  case class SrcFiName(
                        fl_num: String
                      )

  case class TgtFullName(
                          full_name: String
                        )

  case class PrmConfig(esdlStgDb: String,
                       esdlStgTbl: String,
                       certPayDb: String,
                       certPatTable: String,
                       esdlRefDb: String,
                       esdlRefTbl: String,
                       amlReportDb: String,
                       amlReportTbl: String,
                       openDateDb: String,
                       openDatetbl: String

                      )

  case class EsdlAccount(
                        product_type_code: String,
                        ecif_composite_key: String,
                        account_key: String,
                        product_cd: String,
                        status_code: String,
                        account_number: String,
                        holding_branch_key: String,
                        acct_curr_cd: String
                        )


}
