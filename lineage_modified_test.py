import json
from typing import Any, Dict, List, Set, Tuple, Optional

import sqlglot
from sqlglot import exp

# Example SQL - can be replaced with any Hive SQL
SQL_STATEMENTS = """
create table df_prev_conf_me as
select
  number as prev_number,
  lower(state) as prev_state,
  u_closed_at as prev_u_closed_at,
  u_issue_canceled_date as prev_u_issue_canceled_date,
  u_issue_unsuccessful_validation_count as prev_u_issue_unsuccessful_validation_count,
  issue_open_flag as prev_issue_open_flag,
  asofdt as prev_asofdt,
  'previous_monthend' as dt_nm,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517854,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517855,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517856,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,1609,%'
    ) and lower(state) in ('plan', 'mitigate s monitor', 'sustainability', 'validation')
    then 1 else 0
  end as PREV_UDAAP_COMMON_FLAG,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517712,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517710,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,18409693,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,193,%'
    )
    and issue_source = 'Regulator Identified'
    and ISSUE_ACTIVE_FLAG = 1
    then 1 else 0
  end as PREV_FAIR_LENDING_NEW_METRICS_FLAG,
  u_repeat_issue as prev_u_repeat_issue,
  u_issue_risk_level_rating as prev_u_issue_risk_level_rating,
  issue_source as prev_issue_source,
  u_issue_business_activities_due_date as prev_u_issue_business_activities_due_date,
  u_issue_identification_date as prev_u_issue_identification_date
from orc_issue_mgmt_conf.issues A
where A.asofdt = 'prev_monthend_date';



create table df_prev_conf_ye as
select
  number as prev_number,
  lower(state) as prev_state,
  u_closed_at as prev_u_closed_at,
  u_issue_canceled_date as prev_u_issue_canceled_date,
  u_issue_unsuccessful_validation_count as prev_u_issue_unsuccessful_validation_count,
  issue_open_flag as prev_issue_open_flag,
  asofdt as prev_asofdt,
  'previous_monthend' as dt_nm,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517854,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517855,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517856,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,1609,%'
    ) and lower(state) in ('plan', 'mitigate s monitor', 'sustainability', 'validation')
    then 1 else 0
  end as PREV_UDAAP_COMMON_FLAG,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517712,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517710,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,18409693,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,193,%'
    )
    and issue_source = 'Regulator Identified'
    and ISSUE_ACTIVE_FLAG = 1
    then 1 else 0
  end as PREV_FAIR_LENDING_NEW_METRICS_FLAG,
  u_repeat_issue as prev_u_repeat_issue,
  u_issue_risk_level_rating as prev_u_issue_risk_level_rating,
  issue_source as prev_issue_source,
  u_issue_business_activities_due_date as prev_u_issue_business_activities_due_date,
  u_issue_identification_date as prev_u_issue_identification_date
from orc_issue_mgmt_conf.issues A
where A.asofdt = 'prev_yearend_date';


create table df_prev_conf_qe as
select
  number as prev_number,
  lower(state) as prev_state,
  u_closed_at as prev_u_closed_at,
  u_issue_canceled_date as prev_u_issue_canceled_date,
  u_issue_unsuccessful_validation_count as prev_u_issue_unsuccessful_validation_count,
  issue_open_flag as prev_issue_open_flag,
  asofdt as prev_asofdt,
  'previous_monthend' as dt_nm,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517854,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517855,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517856,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,1609,%'
    ) and lower(state) in ('plan', 'mitigate s monitor', 'sustainability', 'validation')
    then 1 else 0
  end as PREV_UDAAP_COMMON_FLAG,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517712,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517710,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,18409693,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,193,%'
    )
    and issue_source = 'Regulator Identified'
    and ISSUE_ACTIVE_FLAG = 1
    then 1 else 0
  end as PREV_FAIR_LENDING_NEW_METRICS_FLAG,
  u_repeat_issue as prev_u_repeat_issue,
  u_issue_risk_level_rating as prev_u_issue_risk_level_rating,
  issue_source as prev_issue_source,
  u_issue_business_activities_due_date as prev_u_issue_business_activities_due_date,
  u_issue_identification_date as prev_u_issue_identification_date
from orc_issue_mgmt_conf.issues A
where A.asofdt = 'prev_quarterend_date';

create table df_prev_conf_we as
select
  number as prev_number,
  lower(state) as prev_state,
  u_closed_at as prev_u_closed_at,
  u_issue_canceled_date as prev_u_issue_canceled_date,
  u_issue_unsuccessful_validation_count as prev_u_issue_unsuccessful_validation_count,
  issue_open_flag as prev_issue_open_flag,
  asofdt as prev_asofdt,
  'previous_monthend' as dt_nm,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517854,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517855,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517856,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,1609,%'
    ) and lower(state) in ('plan', 'mitigate s monitor', 'sustainability', 'validation')
    then 1 else 0
  end as PREV_UDAAP_COMMON_FLAG,
  case
    when (
      concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517712,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,1517710,%'
      or concat(',', replace(u_related_regulation_s_id, ' ', ''), ',') like '%,18409693,%'
      or concat(',', replace(u_related_policies_id, ' ', ''), ',') like '%,193,%'
    )
    and issue_source = 'Regulator Identified'
    and ISSUE_ACTIVE_FLAG = 1
    then 1 else 0
  end as PREV_FAIR_LENDING_NEW_METRICS_FLAG,
  u_repeat_issue as prev_u_repeat_issue,
  u_issue_risk_level_rating as prev_u_issue_risk_level_rating,
  issue_source as prev_issue_source,
  u_issue_business_activities_due_date as prev_u_issue_business_activities_due_date,
  u_issue_identification_date as prev_u_issue_identification_date
from orc_issue_mgmt_conf.issues A
where A.asofdt='prev_weekend_date';


create table df_raw as
select distinct
  raw.assigned_to,
  raw.u_control_management_issue_assignee,
  raw.u_coso_issue,
  raw.u_cr_ql_rationale,
  raw.u_cr_q2_rationale,
  raw.u_cr_q3_rationale,
  raw.u_cr_q4_rationale,
  raw.u_customer_impact,
  raw.u_customer_remediation_ql,
  raw.u_customer_remediation_q2,
  raw.u_customer_remediation_q3,
  raw.u_customer_remediation_q4,
  raw.u_data_issue,
  raw.u_date_legal_review_request_completed,
  raw.u_financial_impact,
  raw.u_issue_business_activities_due_date,
  raw.u_issue_identification_date,
  raw.u_issue_owner_contact,
  raw.u_issue_owner_status,
  raw.u_issue_owning_corporate_risk_hierarchy,
  raw.u_issue_risk_level_rating,
  raw.u_issue_risk_level_rationale,
  raw.u_issue_ryg_status,
  raw.u_issue_sustainability_due_date,
  raw.u_issue_type,
  raw.u_last_issue_completed_activities,
  raw.u_last_issue_created_date,
  raw.u_last_issue_ryg,
  raw.issue_u_last_status_created_by,
  raw.u_law_violation_related_issues,
  raw.u_new_issue_completed_activities,
  raw.u_new_issue_created_date,
  raw.u_new_issue_green_plan,
  raw.u_new_issue_ryg,
  raw.u_new_issue_status_comments,
  raw.u_new_issue_submit_status,
  raw.u_new_issue_upcoming_activities,
  raw.u_new_status_created_by,
  raw.u_other_impacted_corp_risk_hier_units,
  raw.u_other_impacted_corp_risk_hier_units_value,
  raw.u_potential_policy_violation,
  raw.u_pre_vetting_record_id,
  raw.u_primary_auth_source,
  raw.u_primary_risk_sub_type,
  raw.u_primary_risk_type_level_1,
  raw.u_primary_risk_type_level_2,
  raw.u_provide_rationale_accept_risk,
  raw.u_regulator_cited_violation_of_law,
  raw.u_regulatory_impact,
  raw.u_related_legal_entities,
  raw.u_related_issues,
  raw.u_related_policies,
  raw.u_related_regulation_s,
  raw.u_related_risk_drivers,
  raw.u_repeat_issue,
  raw.u_reputation_impact,
  raw.u_secondary_risk_sub_type,
  raw.u_secondary_risk_type_level_1,
  raw.u_secondary_risk_type_level_2,
  raw.u_sensitive_info_inclu_shrp_risk_accept_id,
  raw.watch_list,
  raw.u_regulatory_designated_past_due,
  raw.u_escalated_mra,
  raw.u_management_response_required,
  raw.u_management_response_due_date,
  raw.u_regulatory_engagement_lead,
  raw.u_wfc_expected_completion_date,
  raw.u_original_expected_completion_date,
  raw.u_date_ecd_sent_to_regulator,
  raw.u_regulatory_acknowledged_ecd,
  raw.u_regulator_expected_completion_date,
  raw.risk_event_u_sid_supplemental_issue_data,
  raw.u_aid_issue_business_activities_duedate_extension_count,
  raw.u_sid_issue_business_activities_completed_date,
  raw.u_sid_issue_successful_closure_tollgate_date,
  raw.u_sid_issue_successful_submission_tollgate_date,
  raw.u_sid_issue_sustainability_due_date_extension_count,
  raw.u_sid_shrp_issue_created_date,
  raw.u_sid_first_corrective_action_created_date,
  raw.u_closed_at,
  raw.sys_created_on,
  '25691' as src_sys_id,
  'ITMP' as src_sys_num,
  cast(from_unixtime(unix_timestamp()) as timestamp) as orc_exectn_ts,
  raw.sys_id as sys_id_value,
  raw.active,
  raw.issue_manager,
  raw.closed_at,
  raw.closed_at_value,
  raw.closed_by,
  raw.comments_and_work_notes,
  raw.created_manually,
  raw.description,
  raw.issue_source,
  raw.number,
  raw.opened_at,
  raw.opened_at_value,
  raw.parent,
  raw.parent_issue,
  raw.short_description,
  raw.sla_due,
  raw.state,
  raw.substate,
  raw.sys_created_by,
  raw.sys_id,
  raw.sys_mod_count,
  raw.sys_updated_by,
  raw.sys_updated_on,
  raw.u_accountable_executive,
  raw.u_attach_root_cause_doc,
  raw.u_business_issues,
  raw.u_business_narrative,
  raw.u_ce_override,
  raw.u_child_issues,
  raw.u_cmi_assignee,
  raw.u_completed_rca,
  raw.u_control_executive,
  raw.u_cr_status,
  raw.u_create_interim_cau_customer_remediation_flag,
  raw.u_dependent_issues,
  raw.u_due_date_extension_counter,
  raw.u_fliv_required,
  raw.u_impacted_business,
  raw.u_impacted_business_indicator,
  raw.u_information_requested_by,
  raw.u_interim_corrective_action_id,
  raw.u_ism_status,
  raw.u_issue_canceled_date,
  raw.u_issue_created_by,
  raw.u_issue_identifier,
  raw.u_issue_submitter,
  raw.u_issue_validation_completed_date,
  raw.u_issue_validation_due_date_field,
  raw.u_last_issue_green_plan,
  raw.u_last_issue_status_comments,
  raw.u_last_issue_upcoming_activities,
  raw.u_last_status_created_by,
  raw.u_missed_update_counter,
  raw.u_monthly_status_history_log,
  raw.u_oc_member_override,
  raw.u_operating_committee_member,
  raw.u_orbo,
  raw.u_prior_issues_related_to_repeat_issue,
  raw.u_related_control_records,
  raw.u_related_issues_remed_only_rationale,
  raw.u_remediation_only_issue_flag,
  raw.u_request_information_from,
  raw.u_risk_acceptance_flag,
  raw.u_mat_id,
  raw.u_shrp_issue_id,
  raw.u_source_id,
  raw.u_status_update_exemption,
  raw.u_validation_required,
  raw.work_notes,
  raw.u_source_system_issue_id,
  raw.u_post_validation_required,
  raw.u_issue_identified_date_flagged,
  raw.u_are_there_connected_issues,
  raw.u_accountable_executive_id,
  raw.issue_manager_id,
  raw.u_issue_owner_contact_id,
  raw.u_issue_identifier_id,
  raw.assigned_to_id,
  raw.u_issue_owning_corporate_risk_hierarchy_id,
  raw.u_related_regulation_s_id,
  raw.u_related_control_record_sids,
  raw.u_related_risk_drivers_id,
  raw.u_related_policies_id,
  raw.watch_list_id,
  raw.u_primary_risk_type_level_1_id,
  raw.u_primary_risk_type_level_2_id,
  raw.u_primary_risk_sub_type_id,
  raw.u_secondary_risk_type_level_1_id,
  raw.u_secondary_risk_type_level_2_id,
  raw.u_secondary_risk_sub_type_id,
  raw.u_past_due_rationale,
  raw.u_count_of_applications_mapped,
  raw.u_what_type_of_sensitive_information,
  raw.u_how_many_controls_need_to_be_documented,
  raw.u_risk_instances,
  raw.u_internal_audit_engagement_issue_reference,
  raw.u_irm_connected_issue_challenge,
  raw.u_irm_connected_issue_challenge_rationale,
  raw.u_original_issue_risk_level_rating,
  raw.u_original_issue_validation_due_date,
  raw.u_issue_cancelation_categorization,
  raw.u_issue_opened_date,
  raw.u_regulatory_engagement_lead_id,
  raw.u_risk_instances_id,
  raw.u_related_legal_entities_id,
  raw.u_total_corrective_action_count,
  raw.u_corrective_actions_in_mitigate_phase_count,
  raw.u_corrective_actions_in_plan_phase_count,
  raw.u_corrective_actions_in_sustainability_phase_count,
  raw.u_corrective_actions_in_validate_phase_count,
  raw.u_unsuccessful_validation_date,
  raw.u_issue_sustainability_completed_date,
  raw.u_business_activity_extension,
  raw.u_sustainability_due_date_latest_date,
  raw.u_issue_original_sustainability_due_date,
  raw.u_violation_of_law_count,
  raw.u_issue_unsuccessful_re_validation_percentage,
  raw.u_third_party_validation_required,
  raw.u_third_party_validator_name,
  raw.u_third_party_validation_due_date,
  raw.u_third_party_validation_results,
  raw.u_issue_unsuccessful_fliv_count,
  raw.u_issue_unsuccessful_post_validation_testing_count,
  raw.u_issue_successful_revalidation_percentage,
  raw.u_issue_unsuccessful_revalidation_percentage,
  raw.u_active_corrective_action_count,
  raw.u_active_corrective_actions_in_red_or_yellow_status_count,
  raw.u_sid_issue_business_activities_due_date_missed_flag,
  raw.u_sid_issue_sustainability_due_date_missed_flag,
  year(raw.u_issue_validation_completed_date) = year(raw.asofdt) and lower(raw.state) <> 'canceled' and raw.substate IN ('Closed','Complete','Submitted for Closure') as issue_validation_succes_num_ytd,
  raw.u_issue_days_in_current_substatus_count,
  raw.u_issue_reopened_count,
  raw.u_issue_downgraded_from_green_status_count,
  lower(raw.state) as derv_state,
  datediff(to_date(raw.u_issue_business_activities_due_date), raw.u_issue_opened_date) as derv_u_issue_business_activities_due_date,
  date_format(raw.u_issue_opened_date, 'yyyy') as derv_u_issue_opened_date,
  date_format(raw.asofdt, 'yyyy') as derv_asofdt,
  date_format(raw.u_sid_shrp_issue_created_date, 'yyyy') as derv_u_sid_shrp_issue_created_date,
  lower(raw.issue_source) as derv_issue_source,
  lower(raw.u_issue_risk_level_rating) as derv_u_issue_risk_level_rating,
  lower(raw.u_original_issue_risk_level_rating) as derv_u_original_issue_risk_level_rating,
  datediff(raw.u_issue_sustainability_due_date, raw.asofdt) as derv_u_issue_sustainability_due_date,
  datediff(raw.u_issue_validation_due_date_field, raw.asofdt) as derv_u_issue_validation_due_date_field,
  date_format(raw.u_issue_validation_due_date_field, 'yyyyMM') as derv_u_issue_validation_due_date_field_trim,
  date_format(coalesce(raw.u_closure_tollgate_due_date, raw.u_issue_sustainability_due_date, raw.u_issue_business_activities_due_date), 'yyyyMM') as derv_val_iss_projection_dt,
  datediff(raw.asofdt, raw.u_issue_opened_date) as derv_asofdt_u_issue_opened_date,
  date_format(raw.u_issue_sustainability_completed_date, 'yyyy') as derv_u_issue_sustainability_completed_date,
  date_format(raw.u_business_activity_extension, 'yyyyMM') as derv_u_business_activity_extension,
  date_format(raw.asofdt, 'yyyyMM') as derv_asofdt_2,
  date_format(raw.u_closed_at, 'yyyyMM') as derv_u_closed_at,
  date_format(coalesce(raw.u_issue_validation_due_date_field, raw.u_issue_sustainability_due_date, raw.u_issue_business_activities_due_date), 'yyyyMM') as derv_val_sus_busin_act_due_dt,
  datediff(raw.u_issue_business_activities_due_date, raw.asofdt) as derv_u_issue_business_activities_due_date_2,
  year(raw.u_issue_business_activities_completed_date) as derv_u_issue_business_activities_completed_date,
  raw.asofdt,
  last_day(raw.asofdt) as derv_asofdt2,
  year(raw.asofdt) as derv_asofdt3,
  date_format(raw.u_issue_opened_date, 'yyyyMM') as derv_u_issue_opened_date_2,
  date_format(raw.u_sid_shrp_issue_created_date, 'yyyyMM') as derv_u_sid_shrp_issue_created_date_2,
  raw.u_failed_submission_tollgate_counter,
  raw.u_failed_closure_tollgate_counter,
  raw.u_closure_tollgate_due_date,
  raw.profile,
  raw.u_issue_reopen_date,
  raw.u_issue_business_activities_completed_date,
  raw.u_violation_of_law_title,
  raw.u_primary_auth_source_id,
  raw.additional_assignee_list,
  raw.additional_assignee_list_id,
  raw.opened_by,
  raw.u_potential_violation_of_law,
  raw.u_compliance_review_by,
  raw.u_date_compliance_review_completed,
  raw.u_legal_review_by,
  raw.opened_by_id,
  raw.u_compliance_review_by_id,
  raw.u_legal_review_by_id,
  raw.risk_event_id,
  raw.oc_1,
  raw.oc_2,
  raw.oc_3,
  raw.u_highest_issue_risk_level_rating,
  last_day(coalesce(raw.u_sid_issue_successful_submission_tollgate_date, raw.u_sid_first_corrective_action_created_date)) as derv_u_sid_first_corrective_action_created_date,
  raw.u_is_the_issue_recovery_and_resolution_related,
  last_day(raw.u_sid_issue_successful_submission_tollgate_date) as derv_u_sid_issue_successful_submission_tollgate_date,
  raw.u_select_one_of_the_18_focus_areas,
  raw.u_issue_technology_dependency,
  raw.u_issue_technology_dependency_rationale,
  raw.u_cust_impact_actual_potential,
  raw.u_related_risk_drivers_13,
  raw.u_related_risk_drivers_13_id,
  raw.u_initial_submission_tollgate_response_date,
  raw.u_initial_closure_tollgate_response_date,
  raw.u_reg_comp_impact_rationale,
  raw.u_op_risk_rct_review_by,
  raw.u_op_risk_rct_review_by_id,
  case when concat(',', regexp_replace(coalesce(raw.u_related_regulation_s_id, ''), ' ', ''), ',') like '%,1517854,%'
            or concat(',', regexp_replace(coalesce(raw.u_related_regulation_s_id, ''), ' ', ''), ',') like '%,1517855,%'
            or concat(',', regexp_replace(coalesce(raw.u_related_regulation_s_id, ''), ' ', ''), ',') like '%,1517856,%'
            or concat(',', regexp_replace(coalesce(raw.u_related_policies_id, ''), ' ', ''), ',') like '%,1609,%'
       then 1 else 0 end as curr_udaap_common_flag,
  raw.u_why_1,
  raw.u_why_2,
  raw.u_why_3,
  raw.u_why_4,
  raw.u_why_5,
  raw.u_interim_corrective_action_rationale,
  raw.u_mitigating_action_reduce,
  raw.u_inherent_risk_rating,
  raw.u_specific_citation_attribute_pvol,
  raw.u_pvol_rationale,
  raw.u_audit_contact,
  raw.u_audit_contact_id,
  raw.u_grouped_id,
  raw.u_cr_substate,
  raw.u_cr_substate_id,
  raw.u_change_request_type,
  raw.u_change_request_type_id,
  raw.u_irm_final_reg_comp_impact_rating,
  lower(raw.u_issue_risk_level_rating) as derv_issue_calc_risk_level_rating,
  raw.u_last_issue_status_approved_date,
  raw.u_grouped_issues,
  raw.u_is_there_actual_customer_impact,
  raw.u_issue_most_recent_ry_status_date,
  raw.u_prior_issue_risk_level_rating,
  case
    when year(raw.u_issue_business_activities_completed_date) = year(raw.asofdt)
      and month(raw.u_issue_business_activities_completed_date) = month(raw.asofdt)
      and raw.u_issue_business_activities_completed_date <= raw.u_issue_original_business_activities_due_date
      and lower(raw.state) in ('sustainability','validation','closed')
    then 1 else 0 end as issue_bus_activ_orig_comp_on_time_flag,
  case
    when year(raw.u_issue_business_activities_completed_date) = year(raw.asofdt)
      and month(raw.u_issue_business_activities_completed_date) = month(raw.asofdt)
      and lower(raw.state) in ('sustainability','validation','closed')
      and raw.u_issue_business_activities_due_date is not null
    then 1 else 0 end as issue_bus_activ_comp_mtd_flag,
  case
    when year(raw.u_issue_validation_completed_date) = year(raw.asofdt)
      and month(raw.u_issue_validation_completed_date) = month(raw.asofdt)
      and raw.substate in ('Closed','Complete','Submitted for Closure')
      and lower(raw.state) <> 'canceled'
    then 1 else 0 end as issue_validation_succes_num_mtd,
  
  pmld.prev_issue_open_flag as prev_issue_open_flag,
  case when raw.u_risk_mitigated_at_issue_level is null or raw.u_risk_mitigated_at_issue_level = 'None' then null else raw.u_risk_mitigated_at_issue_level end as u_risk_mitigated_at_issue_level,
  prevdt.prev_u_issue_unsuccessful_validation_count as prevdt_u_issue_unsuccessful_validation_count,
  prevdt.prev_asofdt as prev_dates_dt,
  raw.u_tech_dependency_group,
  raw.u_tech_dependent_implementation_actual_customer_impact_stop_date,
  raw.u_ca_addressing_cust_impact,
  raw.u_issue_cancellation_rationale,
  raw.u_legal_hold_id,
  raw.u_due_date_missed_sla_rationale,
  case
    when lower(raw.state) = 'closed'
      and date_format(raw.u_closed_at,'yyyyMM') = date_format(raw.asofdt,'yyyyMM')
      and coalesce(pmld.prev_state,'') not in ('closed','canceled')
    then 1 else 0 end as issue_closed_inmonth_flag,
  case
    when lower(raw.state) = 'canceled'
      and date_format(raw.u_issue_canceled_date,'yyyyMM') = date_format(raw.asofdt,'yyyyMM')
      and coalesce(pmld.prev_state,'') not in ('closed','canceled')
    then 1 else 0 end as issue_cancel_inmonth_flag,
  case
    when lower(raw.state) = 'validation'
      and pmld.prev_state in ('plan','mitigate & monitor')
    then 1 else 0 end as issue_bus_activ_to_valid_inmonth_flag,
  case
    when lower(raw.state) = 'closed'
      and pmld.prev_state in ('plan','mitigate & monitor')
    then 1 else 0 end as issue_bus_activ_to_closed_inmonth_flag,
  
from orc_issue_mgmt_raw.issues raw
left join df_prev_conf_me pmld on pmld.prev_number = raw.number
left join df_prev_conf_ye pyld on pyld.prev_number = raw.number
left join df_prev_conf_qe prevq on prevq.prev_number = raw.number
left join df_prev_conf_we prevdt on prevdt.prev_number = raw.number
where raw.asofdt = '${hivevar:source_date}'
  and raw.number is not null
  and raw.number <> ''
  and lower(trim(raw.u_issue_hold_file)) <> 'yes'
;


CREATE TABLE derived_df AS
SELECT
    raw.number AS number_1,
    -- 1. issue_active_due_date_lplus_years_flag
    CASE
        WHEN raw.derv_state IN ('mitigate & monitor', 'sustainability', 'validation')
        AND raw.derv_u_issue_business_activities_due_date > 365
        AND raw.u_issue_business_activities_due_date IS NOT NULL
        THEN 1
        ELSE 0
    END AS issue_active_due_date_lplus_years_flag,

    -- 2. issue_new_ytd_flag (Assuming date comparison is for the same year)
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.derv_asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        THEN 1
        ELSE 0
    END AS issue_new_ytd_flag,

    -- 3. issue_new_ytd_c_h_selfid_flag
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.derv_asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        AND raw.derv_issue_source = 'self-identified'
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
		AND raw.issue_closed_inmonth_flag=1
        THEN 1
        ELSE 0
    END AS issue_new_ytd_c_h_selfid_flag,

    -- 4. issue_new_ytd_c_h_flag
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.derv_asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        THEN 1
        ELSE 0
    END AS issue_new_ytd_c_h_flag,

    -- 5. issue_new_ytd_m_l_selfid_flag
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.derv_asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        AND raw.derv_issue_source = 'self-identified'
        AND raw.derv_issue_calc_risk_level_rating IN ('low', 'moderate')
        THEN 1
        ELSE 0
    END AS issue_new_ytd_m_l_selfid_flag,

    -- 6. issue_new_ytd_m_l_flag (Renamed from issue_new_ytd_=_1_flag)
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.derv_asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        AND raw.derv_issue_calc_risk_level_rating IN ('low', 'moderate')
        THEN 1
        ELSE 0
    END AS issue_new_ytd_m_l_flag,

    -- 7. issue_active_c_h_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        THEN 1
        ELSE 0
    END AS issue_active_c_h_flag,

    -- 8. issue_active_m_l_flag (Renamed from issue_active_m_1_flag)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND raw.derv_issue_calc_risk_level_rating IN ('low', 'moderate')
        THEN 1
        ELSE 0
    END AS issue_active_m_l_flag,

    -- 9. issue_open_selfid_flag (Assuming derv_issue_source needs an operator)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        AND raw.derv_issue_source = 'self-identified'
        THEN 1
        ELSE 0
    END AS issue_open_selfid_flag,

    -- 10. issue_open_c_h_selfid_flag (The logic here was badly broken, I corrected the syntax and removed the duplicated/misplaced condition)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        AND raw.derv_issue_source = 'self-identified'
        THEN 1
        ELSE 0
    END AS issue_open_c_h_selfid_flag,

    -- 11. issue_open_c_h_issue_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        THEN 1
        ELSE 0
    END AS issue_open_c_h_issue_flag,

    -- 12. issue_sustainability_due_date_bucket
    CASE
        WHEN raw.derv_state NOT IN ('plan', 'mitigate & monitor', 'sustainability') THEN NULL
        WHEN raw.derv_state = 'sustainability' AND LOWER(raw.substate) <> 'sustainability' THEN NULL
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability') OR (raw.derv_state = 'sustainability' AND LOWER(raw.substate) = 'sustainability')
        THEN
            CASE
                WHEN raw.u_issue_sustainability_due_date IS NULL THEN 'No Due Date'
                WHEN raw.derv_u_issue_sustainability_due_date <= 0 THEN 'Past Due'
                WHEN raw.derv_u_issue_sustainability_due_date < 31 THEN '1-30 Days'
                WHEN raw.derv_u_issue_sustainability_due_date < 61 THEN '31-60 Days'
                WHEN raw.derv_u_issue_sustainability_due_date < 91 THEN '61-90 Days'
                WHEN raw.derv_u_issue_sustainability_due_date < 366 THEN '91-365 Days'
                ELSE '366+ Days'
            END
        ELSE NULL
    END AS issue_sustainability_due_date_bucket,

    -- 13. issue_days_until_validation_due_date
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND raw.u_issue_validation_due_date_field > raw.asofdt
        THEN raw.derv_u_issue_validation_due_date_field
        ELSE NULL
    END AS issue_days_until_validation_due_date,

    -- 14. issue_validation_due_date_bucket
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        THEN
            CASE
                WHEN raw.u_issue_validation_due_date_field IS NULL THEN 'No Due Date'
                WHEN raw.derv_u_issue_validation_due_date_field <= 0 THEN 'Past Due'
                WHEN raw.derv_u_issue_validation_due_date_field < 31 THEN '1-30 Days'
                WHEN raw.derv_u_issue_validation_due_date_field < 61 THEN '31-60 Days'
                WHEN raw.derv_u_issue_validation_due_date_field < 91 THEN '61-90 Days'
                WHEN raw.derv_u_issue_validation_due_date_field < 366 THEN '91-365 Days'
                ELSE '366+ Days'
            END
        ELSE NULL
    END AS issue_validation_due_date_bucket,

    -- 15. issue_days_aged (Assuming derv_asofdt_u_issue_opened_date is a difference in days)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        THEN raw.derv_asofdt_u_issue_opened_date
        ELSE NULL
    END AS issue_days_aged,

    -- 16. issue_new_ytd_selfid_flag (Assuming date comparison is for the same year)
    CASE
        WHEN YEAR(COALESCE(raw.derv_u_issue_opened_date, raw.derv_u_sid_shrp_issue_created_date)) = YEAR(raw.asofdt)
        AND raw.derv_state NOT IN ('canceled', 'capture')
        AND raw.derv_issue_source = 'self-identified'
        THEN 1
        ELSE 0
    END AS issue_new_ytd_selfid_flag,

    -- 17. issue_selfid_active_flag (Assuming derv_issue_source needs an operator)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND raw.derv_issue_source = 'self-identified'
        THEN 1
        ELSE 0
    END AS issue_selfid_active_flag,

    -- 18. issue_active_ryg_ry_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND LOWER(raw.u_issue_ryg_status) IN ('yellow', 'red')
        THEN 1
        ELSE 0
    END AS issue_active_ryg_ry_flag,

    -- 19. issue_open_past_due_flag
    CASE
        WHEN raw.derv_state = 'mitigate & monitor'
        AND raw.u_issue_business_activities_due_date <= raw.asofdt
        THEN 1
        ELSE 0
    END AS issue_open_past_due_flag,

    -- 20. issue_open_past_due_ch_flag
    CASE
        WHEN raw.derv_state = 'mitigate & monitor'
        AND raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        AND raw.u_issue_business_activities_due_date < raw.asofdt
        THEN 1
        ELSE 0
    END AS issue_open_past_due_ch_flag,

    -- 21. issue_open_m_l_selfid_flag (Renamed from issue_open_m_1_selfid_flag)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        AND raw.derv_issue_calc_risk_level_rating IN ('low', 'moderate')
        AND raw.derv_issue_source = 'self-identified'
        THEN 1
        ELSE 0
    END AS issue_open_m_l_selfid_flag,

    -- 22. issue_open_m_l_flag (Renamed from issue_open_m_1_flag)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        AND raw.derv_issue_calc_risk_level_rating IN ('low', 'moderate')
        THEN 1
        ELSE 0
    END AS issue_open_m_l_flag,

    -- 23. issue_active_2plus_bus_activ_due_date_ext (Assuming date comparison is for the same year)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND YEAR(raw.asofdt) = YEAR(raw.asofdt) -- This condition is likely wrong in the original but I kept it, assuming `YEAR(asofdt)` should compare to a date in the current row
        AND raw.u_sid_issue_business_activities_duedate_extension_count >= 2
        THEN 1
        ELSE 0
    END AS issue_active_2plus_bus_activ_due_date_ext,

    -- 24. issue_failing_validation_inquarter_flag (Assuming missing comparison operators are equality)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND YEAR(raw.u_unsuccessful_valdation_date) = YEAR(raw.asofdt)
        AND QUARTER(raw.u_unsuccessful_valdation_date) = QUARTER(raw.asofdt)
        THEN 1
        ELSE 0
    END AS issue_failing_validation_inquarter_flag,

    -- 25. issue_validation_fail_trigger_month (Assuming missing comparison operators are equality)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND YEAR(raw.u_unsuccessful_valdation_date) = YEAR(raw.asofdt)
        AND MONTH(raw.u_unsuccessful_valdation_date) = MONTH(raw.asofdt)
        THEN 1
        ELSE 0
    END AS issue_validation_fail_trigger_month,

    -- 26. issue_active_lplus_bus_activity_due_date_ext_flag (Assuming u_sid_issue_business_activities_duedate_extension_count > 0)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        AND raw.u_sid_issue_business_activities_duedate_extension_count > 0
        THEN 1
        ELSE 0
    END AS issue_active_lplus_bus_activity_due_date_ext_flag,

    -- 27. issue_bus_activ_orig_comp_ontime_ytd_flag (Assuming date comparison is for the same year and u_issue_business_activities_completed_date < u_issue_original_business_activities_due_date)
    CASE
        WHEN YEAR(raw.derv_u_issue_business_activities_completed_date) = YEAR(raw.asofdt)
        AND raw.u_issue_business_activities_completed_date <= raw.u_issue_original_business_activities_due_date -- Changed < to <= for completeness
        AND raw.derv_state IN ('closed', 'sustainability', 'validation')
        THEN 1
        ELSE 0
    END AS issue_bus_activ_orig_comp_ontime_ytd_flag,

    -- 28. issue_sust_orig_comp_ontime_ytd_flag (Assuming date comparison is equality with asofdt, and completed <= original due date)
    CASE
        WHEN raw.derv_u_issue_sustainability_completed_date = raw.derv_asofdt
        AND raw.u_issue_sustainability_completed_date <= raw.u_issue_original_sustainability_due_date
        AND raw.derv_state IN ('validation', 'closed')
        THEN 1
        ELSE 0
    END AS issue_sust_orig_comp_ontime_ytd_flag,

    -- 29. issue_bus_activ_due_date_ext (Assuming derv_u_business_activity_extension is a date, and comparison is to derv_asofdt_2)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation', 'closed')
        AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2
        THEN 1
        ELSE 0
    END AS issue_bus_activ_due_date_ext,

    -- 30. issue_ch_inmonth_due_date_ext
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation', 'closed')
        AND raw.derv_u_issue_risk_level_rating IN ('critical', 'high')
        AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2
        THEN 1
        ELSE 0
    END AS issue_ch_inmonth_due_date_ext,

    -- 31. issue_ml_inmonth_due_date_ext
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation', 'closed')
        AND raw.derv_u_issue_risk_level_rating IN ('low', 'moderate')
        AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2
        THEN 1
        ELSE 0
    END AS issue_ml_inmonth_due_date_ext,

    -- 32. issue_closed_early_inmonth_flag (First instance corrected: u_validation_required='No' and NOT (closed/closed_at=asofdt_2) logic appears flawed, assuming standard closure logic)
    CASE
        WHEN raw.derv_state NOT IN ('capture', 'canceled')
        AND raw.derv_issue_source <> 'regulator identified'
        THEN
            CASE
                WHEN raw.u_validation_required = 'No'
                AND raw.derv_Val_iss_projection_dt = raw.derv_asofdt_2
                AND NOT (raw.derv_state = 'closed' AND raw.derv_u_closed_at <> raw.derv_asofdt_2) THEN 1 -- Retained the complex original logic
                WHEN raw.u_validation_required = 'Yes'
                AND raw.derv_state IN ('validation', 'closed')
                AND raw.derv_u_issue_validation_due_date_field_trim > raw.derv_asofdt_2 THEN 1
                ELSE 0
            END
        ELSE 0
    END AS issue_closed_early_inmonth_flag_1, -- Renamed to differentiate from the second instance

    -- 33. issue_closed_early_inmonth_flag (Second instance corrected)
    CASE
        WHEN raw.derv_state = 'closed'
        AND raw.derv_u_closed_at = raw.derv_asofdt_2
        AND raw.derv_issue_source <> 'regulator identified'
        THEN
            CASE
                WHEN raw.u_validation_required = 'No' AND raw.derv_Val_iss_projection_dt > raw.derv_asofdt_2 THEN 1
                WHEN raw.u_validation_required = 'Yes' AND raw.derv_u_issue_validation_due_date_field_trim > raw.derv_asofdt_2 THEN 1
                ELSE 0
            END
        ELSE 0
    END AS issue_closed_early_inmonth_flag, -- Using the original name for the more specific flag

    -- 34. issue_closed_late_inmonth_flag
    CASE
        WHEN raw.derv_state = 'closed'
        AND raw.derv_u_closed_at = raw.derv_asofdt_2
        AND raw.derv_issue_source <> 'regulator identified'
        THEN
            CASE
                WHEN raw.u_validation_required = 'No' AND raw.derv_val_iss_projection_dt < raw.derv_asofdt_2 THEN 1
                WHEN raw.u_validation_required = 'Yes' AND raw.derv_u_issue_validation_due_date_field_trim < raw.derv_asofdt_2 THEN 1
                ELSE 0
            END
        ELSE 0
    END AS issue_closed_late_inmonth_flag,

    -- 35. issue_closed_inprojected_flag
    CASE
        WHEN raw.derv_state = 'closed'
        AND raw.derv_u_closed_at = raw.derv_asofdt_2
        AND raw.derv_issue_source <> 'regulator identified'
        THEN
            CASE
                WHEN raw.u_validation_required = 'No' AND raw.derv_val_iss_projection_dt = raw.derv_asofdt_2 THEN 1
                WHEN raw.u_validation_required = 'Yes' AND raw.derv_u_issue_validation_due_date_field_trim = raw.derv_asofdt_2 THEN 1
                ELSE 0
            END
        ELSE 0
    END AS issue_closed_inprojected_flag,

    -- 36. issue_repeate_opened_inmonth_flag (Assuming date comparison is for the same year and month)
    CASE
        WHEN raw.derv_state <> 'canceled'
        AND LOWER(raw.u_repeat_issue) = 'yes'
        AND DATE_FORMAT(raw.u_issue_opened_date, 'yyyyMM') = DATE_FORMAT(raw.derv_asofdt_2, 'yyyyMM') -- Corrected date format comparison
        THEN 1
        ELSE 0
    END AS issue_repeate_opened_inmonth_flag,

    -- 37. issue_ch_flag
    CASE
        WHEN raw.derv_issue_calc_risk_level_rating IN ('critical', 'high')
        THEN 1
        ELSE 0
    END AS issue_ch_flag,

    -- 38. issue_ml_flag
    CASE
        WHEN raw.derv_issue_calc_risk_level_rating IN ('moderate', 'low')
        THEN 1
        ELSE 0
    END AS issue_ml_flag,

    -- 39. issue_bus_activ_comp_ontime_ytd_flag
    CASE
        WHEN YEAR(raw.derv_u_issue_business_activities_completed_date) = YEAR(raw.asofdt)
        AND raw.u_issue_business_activities_completed_date <= raw.u_issue_business_activities_due_date
        AND raw.derv_state IN ('sustainability', 'validation', 'closed')
        THEN 1
        ELSE 0
    END AS issue_bus_activ_comp_ontime_ytd_flag,

    -- 40. issue_bus_activ_comp_ytd_flag
    CASE
        WHEN YEAR(raw.derv_u_issue_business_activities_completed_date) = YEAR(raw.asofdt)
        AND raw.derv_state IN ('sustainability', 'validation', 'closed')
        AND raw.u_issue_business_activities_due_date IS NOT NULL
        THEN 1
        ELSE 0
    END AS issue_bus_activ_comp_ytd_flag,

    -- 41. issue_ml_inmonth_ext_den_flag
    CASE
        WHEN (raw.derv_state IN ('plan', 'mitigate & monitor') OR (raw.derv_state IN ('sustainability', 'validation', 'closed') AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2))
        AND raw.derv_u_issue_risk_level_rating IN ('low', 'moderate')
        THEN 1
        ELSE 0
    END AS issue_ml_inmonth_ext_den_flag,

    -- 42. issue_inmonth_ext_den_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor') OR (raw.derv_state IN ('sustainability', 'validation', 'closed') AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2)
        THEN 1
        ELSE 0
    END AS issue_inmonth_ext_den_flag,

    -- 43. issue_ch_inmonth_ext_den_flag
    CASE
        WHEN (raw.derv_state IN ('plan', 'mitigate & monitor') OR (raw.derv_state IN ('sustainability', 'validation', 'closed') AND raw.derv_u_business_activity_extension = raw.derv_asofdt_2))
        AND raw.derv_u_issue_risk_level_rating IN ('critical', 'high')
        THEN 1
        ELSE 0
    END AS issue_ch_inmonth_ext_den_flag,

    -- 44. issue_inqrtr_ext_flag (Assuming missing comparison is for the same quarter and year)
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation', 'closed')
        AND CONCAT_WS('/', CAST(QUARTER(raw.u_business_activity_extension) AS STRING), CAST(YEAR(raw.u_business_activity_extension) AS STRING)) = CONCAT_WS('/', CAST(QUARTER(raw.asofdt) AS STRING), CAST(YEAR(raw.asofdt) AS STRING)) -- Corrected date part extraction and comparison
        THEN 1
        ELSE 0
    END AS issue_inqrtr_ext_flag,

    -- 45. issue_active_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor', 'sustainability', 'validation')
        THEN 1
        ELSE 0
    END AS issue_active_flag,

    -- 46. issue_open_flag
    CASE
        WHEN raw.derv_state IN ('plan', 'mitigate & monitor')
        THEN 1
        ELSE 0
    END AS issue_open_flag,

    -- 47. issue_days_until_bus_activ_due_date (Assuming derv_derv_u_issue_business_activities_due_date_2 is a calculated days difference)
    CASE
        WHEN raw.derv_state = 'mitigate & monitor'
        AND raw.u_issue_business_activities_due_date > raw.asofdt
        THEN raw.derv_derv_u_issue_business_activities_due_date_2
        ELSE NULL
    END AS issue_days_until_bus_activ_due_date,

    -- 48. issue_bus_activ_due_date_bucket (Assuming derv_derv_u_issue_business_activities_due_date_2 is the days difference)
    CASE
        WHEN raw.derv_state NOT IN ('mitigate & monitor') THEN NULL
        WHEN raw.derv_state = 'mitigate & monitor'
        THEN
            CASE
                WHEN raw.u_issue_business_activities_due_date IS NULL THEN 'No Due Date'
                WHEN raw.derv_derv_u_issue_business_activities_due_date_2 <= 0 THEN 'Past Due'
                WHEN raw.derv_derv_u_issue_business_activities_due_date_2 < 31 THEN '1-30 Days'
                WHEN raw.derv_derv_u_issue_business_activities_due_date_2 < 61 THEN '31-60 Days'
                WHEN raw.derv_derv_u_issue_business_activities_due_date_2 < 91 THEN '61-90 Days'
                WHEN raw.derv_derv_u_issue_business_activities_due_date_2 < 121 THEN '91-120 Days' -- Used 121 from the next line's ELSE/WHEN for consistency
                ELSE '121+ Days'
            END
        ELSE NULL
    END AS issue_bus_activ_due_date_bucket,

    -- 49. issue_sustain_validation_closed_canceled_inquarter_flag (Corrected logic, assuming date comparison is for same quarter/year)
    CASE
        WHEN raw.derv_state IN ('sustainability', 'validation', 'closed')
        AND YEAR(raw.derv_u_issue_business_activities_completed_date) = YEAR(raw.derv_asofdt)
        AND QUARTER(raw.derv_u_issue_business_activities_completed_date) = QUARTER(raw.asofdt)
        THEN 1
        WHEN raw.derv_state = 'canceled'
        AND raw.prev_issue_open_flag = 1 -- Assuming this column exists in 'df_raw' or is calculated earlier
        THEN 1
        ELSE 0
    END AS issue_sustain_validation_closed_canceled_inquarter_flag

FROM
    df_raw raw
LEFT OUTER JOIN
    (
        SELECT
            org_id,
            asofdt AS crh_asofdt,
            l2_name,
            l3_name
        FROM
            orc_issue_mgmt_conf.fc_corp_risk_hrchy
        WHERE
            org_id IS NOT NULL AND org_id <> ''
    ) corprskhier
ON
    raw.u_issue_owning_corporate_risk_hierarchy_id = corprskhier.org_id
    AND corprskhier.org_id IS NOT NULL
    AND raw.asofdt = corprskhier.crh_asofdt
LEFT JOIN
    (
        SELECT
            u_number,
            u_shrp_source_id,
            u_regulatory_agency
        FROM
            orc_issue_mgmt_raw.issue_sources
        WHERE
            asofdt = 'SSOURCE_DATE' -- Placeholder, likely meant to be a variable like CURRENT_DATE or raw.asofdt
    ) src
ON
    raw.u_source_id = src.u_number;

"""

def parse_statements(sql: str) -> List[exp.Expression]:
    """Parse SQL statements with Hive dialect"""
    try:
        return sqlglot.parse(sql, read="hive")
    except Exception as e:
        print(f"Warning: Failed to parse SQL: {e}")
        return []


def extract_target_and_select(statement: exp.Expression) -> Tuple[Optional[str], Optional[exp.Select]]:
    """Extract target table and SELECT expression from CREATE/INSERT statements"""
    target_table = None
    select_exp = None
    try:
        if isinstance(statement, exp.Create):
            target_table = statement.this.sql() if getattr(statement, "this", None) else None
            select_exp = getattr(statement, "expression", None)
        elif isinstance(statement, exp.Insert):
            target_table = statement.this.sql() if getattr(statement, "this", None) else None
            select_exp = getattr(statement, "expression", None)
        
        if isinstance(select_exp, exp.Select):
            return target_table, select_exp
    except Exception as e:
        print(f"Warning: Error extracting target/select: {e}")
    
    return target_table, None


def _clean_table_name(tbl_expr: exp.Expression) -> str:
    """Return fully qualified table name without alias (keeps db/schema if present)."""
    try:
        # tbl_expr.sql() retains catalog/schema; strip any trailing alias tokens
        raw = tbl_expr.sql()
    except Exception:
        raw = tbl_expr.sql() if hasattr(tbl_expr, "sql") else ""
    return raw.split()[0]


def extract_source_map(select_exp: exp.Select) -> Tuple[Dict[str, str], Dict[str, List[Tuple[str, str]]]]:
    """
    Build alias -> table name mapping and track subquery column aliases.
    Handles CTEs, subqueries, and regular table references.
    """
    src_map: Dict[str, str] = {}
    subquery_cols_map: Dict[str, List[Tuple[str, str]]] = {}
    
    try:
        # Handle CTEs (WITH clauses)
        with_clause = select_exp.args.get("with")
        if with_clause:
            expressions = with_clause.expressions if hasattr(with_clause, "expressions") else []
            for cte in expressions:
                cte_alias = cte.alias if hasattr(cte, "alias") else None
                if cte_alias:
                    inner_select = cte.this if isinstance(cte.this, exp.Select) else None
                    if inner_select:
                        inner_map, inner_sq = extract_source_map(inner_select)
                        src_map.update(inner_map)
                        subquery_cols_map.update(inner_sq)
        
        from_clause = select_exp.args.get("from_")
        if from_clause and getattr(from_clause, "this", None):
            tbl = from_clause.this
            if isinstance(tbl, exp.Subquery):
                # Dynamically get subquery alias
                sq_alias = getattr(tbl, "alias_or_name", None) or getattr(tbl, "alias", None)
                if not sq_alias:
                    sq_alias = f"subquery_{id(tbl)}"
                
                inner_select = tbl.this if isinstance(tbl.this, exp.Select) else None
                if inner_select:
                    col_pairs: List[Tuple[str, str]] = []
                    for proj in inner_select.expressions:
                        if isinstance(proj, exp.Star):
                            continue
                        src_col = proj.this.name if isinstance(proj.this, exp.Column) else proj.this.sql()
                        tgt_col = proj.alias_or_name or src_col
                        col_pairs.append((src_col, tgt_col))
                    if col_pairs:
                        subquery_cols_map[sq_alias] = col_pairs
                    inner_map, inner_sq = extract_source_map(inner_select)
                    src_map.update(inner_map)
                    subquery_cols_map.update(inner_sq)
            else:
                table_name_clean = _clean_table_name(tbl)
                alias = getattr(tbl, "alias_or_name", None) or getattr(tbl, "alias", None) or table_name_clean
                src_map[alias] = table_name_clean
        
        # Handle JOINs
        for join in select_exp.args.get("joins") or []:
            if getattr(join, "this", None):
                jt = join.this
                if isinstance(jt, exp.Subquery):
                    # Dynamically get subquery alias
                    sq_alias = getattr(jt, "alias_or_name", None) or getattr(jt, "alias", None)
                    if not sq_alias:
                        sq_alias = f"subquery_{id(jt)}"
                    
                    inner_select = jt.this if isinstance(jt.this, exp.Select) else None
                    if inner_select:
                        col_pairs: List[Tuple[str, str]] = []
                        for proj in inner_select.expressions:
                            if isinstance(proj, exp.Star):
                                continue
                            src_col = proj.this.name if isinstance(proj.this, exp.Column) else proj.this.sql()
                            tgt_col = proj.alias_or_name or src_col
                            col_pairs.append((src_col, tgt_col))
                        if col_pairs:
                            subquery_cols_map[sq_alias] = col_pairs
                        inner_map, inner_sq = extract_source_map(inner_select)
                        src_map.update(inner_map)
                        subquery_cols_map.update(inner_sq)
                else:
                    table_name_clean = _clean_table_name(jt)
                    jalias = getattr(jt, "alias_or_name", None) or getattr(jt, "alias", None) or table_name_clean
                    src_map[jalias] = table_name_clean
    except Exception as e:
        print(f"Warning: Error in extract_source_map: {e}")
    
    return src_map, subquery_cols_map


def get_column_parts(col_exp: exp.Expression) -> Tuple[Optional[str], Optional[str]]:
    """Extract table alias and column name from column expression"""
    try:
        if isinstance(col_exp, exp.Column):
            # Prefer direct properties; fall back to args
            table_alias = getattr(col_exp, "table", None)
            if not table_alias and col_exp.args.get("table") is not None:
                try:
                    table_alias = col_exp.args["table"].name
                except Exception:
                    table_alias = str(col_exp.args.get("table"))

            name = None
            try:
                name = getattr(col_exp.this, "name", None)
            except Exception:
                name = getattr(col_exp, "name", None)
            if not name:
                try:
                    name = col_exp.name
                except Exception:
                    name = None
            # Fallback: if name is still None, use the rendered SQL (e.g., for dotted or quoted columns)
            if name is None:
                try:
                    raw_sql = col_exp.sql()
                except Exception:
                    raw_sql = None
                if raw_sql:
                    if "." in raw_sql:
                        # e.g., raw.active -> alias raw, column active
                        name = raw_sql.split(".")[-1]
                        if table_alias is None:
                            table_alias = raw_sql.rsplit(".", 1)[0]
                    else:
                        name = raw_sql
            return table_alias, name
    except Exception:
        pass
    return None, None


def parse_simple_create_table_as_select(sql: str) -> Optional[Dict[str, Any]]:
    """
    Fallback parser for simple CREATE TABLE AS SELECT statements using regex.
    Returns source and target column mappings when sqlglot fails to extract columns properly.
    """
    import re
    
    sql = sql.strip()
    
    # Extract target table name
    target_match = re.search(r'create\s+table\s+(\w+)\s+as', sql, re.IGNORECASE)
    if not target_match:
        return None
    
    target_table = target_match.group(1)
    
    # Extract SELECT clause (columns)
    select_match = re.search(r'select\s+(.*?)\s+from', sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return None
    
    columns_str = select_match.group(1)
    # Simple split by comma (not perfect for complex expressions, but good for simple cases)
    columns = [col.strip() for col in columns_str.split(',')]
    
    # Extract source table
    from_match = re.search(r'from\s+([\w.]+)(?:\s+(\w+))?', sql, re.IGNORECASE)
    if not from_match:
        return None
    
    source_table = from_match.group(1)
    alias = from_match.group(2) if from_match.group(2) else None
    
    # Parse source and target columns
    source_columns = []
    target_columns = []
    
    for col in columns:
        # Handle alias.column or column
        if '.' in col:
            parts = col.split('.')
            source_col = parts[-1]  # Get column name after alias
        else:
            source_col = col
        
        # Check if there's a column alias (AS)
        if ' as ' in col.lower():
            col_parts = re.split(r'\s+as\s+', col, flags=re.IGNORECASE)
            source_col = col_parts[0].split('.')[-1] if '.' in col_parts[0] else col_parts[0]
            target_col = col_parts[1]
        else:
            target_col = source_col
        
        source_columns.append({
            'table': source_table,
            'column': source_col.strip()
        })
        
        target_columns.append({
            'table': target_table,
            'column': target_col.strip()
        })
    
    return {
        'source_table': source_table,
        'source_alias': alias,
        'target_table': target_table,
        'source_columns': source_columns,
        'target_columns': target_columns
    }


def is_valid_column_name(name: str) -> bool:
    """Check if a string is a valid column name (not a literal, keyword, or malformed)"""
    if not name or not isinstance(name, str):
        return False
    
    name = name.strip()
    
    # Filter out empty or very short names
    if len(name) == 0:
        return False
    
    # Filter out string literals (including escaped quotes)
    if name.startswith("'") or name.startswith('"') or "'" in name or '"' in name:
        return False
    
    # Filter out numbers
    if name.isdigit():
        return False
    
    # Filter out names with parentheses (function calls or incomplete expressions)
    if '(' in name or ')' in name:
        return False
    
    # Filter out names with SQL operators or special characters that indicate expressions
    invalid_chars = ['%', ',', ';', '<', '>', '=', '!', '?', '|', '&', '^', '~', '`']
    if any(char in name for char in invalid_chars):
        return False
    
    # Filter out multi-line strings or strings with newlines
    if '\n' in name or '\r' in name or '\t' in name:
        return False
    
    # Filter out SQL keywords and common functions
    sql_keywords = {
        'case', 'when', 'then', 'else', 'end', 'and', 'or', 'not', 'in', 'like',
        'is', 'null', 'select', 'from', 'where', 'group', 'by', 'order', 'having',
        'as', 'on', 'join', 'left', 'right', 'inner', 'outer', 'cross', 'union',
        'concat', 'replace', 'lower', 'upper', 'trim', 'cast', 'substring', 'coalesce',
        'validation', 'plan', 'mitigate', 'monitor', 'sustainability'
    }
    if name.lower() in sql_keywords:
        return False
    
    # Filter out malformed names with only special characters
    if all(c in "()[]{},.;:!@#$%^&*-+=<>?/\\|`~' \t\n\r" for c in name):
        return False
    
    # Must start with letter or underscore (valid identifier pattern)
    if not (name[0].isalpha() or name[0] == '_'):
        return False
    
    return True


def extract_columns_from_expression(expr_sql: str) -> List[Tuple[Optional[str], Optional[str]]]:
    """Parse expression and extract column references (alias, column_name)"""
    cols: List[Tuple[Optional[str], Optional[str]]] = []
    try:
        node = sqlglot.parse_one(expr_sql, read="hive")
        for c in node.find_all(exp.Column):
            ta, cn = get_column_parts(c)
            # Only add valid column names
            if cn and is_valid_column_name(cn):
                cols.append((ta, cn))
    except Exception:
        pass
    return cols


def build_table_defs(expressions: List[exp.Expression]) -> Dict[str, Dict[str, Any]]:
    """Build lineage definitions for all CREATE/INSERT statements"""
    table_defs: Dict[str, Dict[str, Any]] = {}

    def resolve_base_sources_local(tbl: str, seen: Optional[Set[str]] = None) -> Set[str]:
        """Recursively resolve intermediate tables to base physical tables"""
        if seen is None:
            seen = set()
        if tbl in seen:
            return set()
        seen.add(tbl)
        if tbl not in table_defs:
            return {tbl}
        declared = table_defs[tbl].get("source_tables", [])
        if not declared:
            return {tbl}
        bases: Set[str] = set()
        for s in declared:
            if s == tbl:
                bases.add(s)
            else:
                bases.update(resolve_base_sources_local(s, seen))
        return bases

    for stmt in expressions:
        try:
            if not isinstance(stmt, (exp.Create, exp.Insert)):
                continue
            
            target_table, select_exp = extract_target_and_select(stmt)
            if not target_table or not select_exp:
                continue

            source_map, subquery_cols_map = extract_source_map(select_exp)
            immediate_sources = list(source_map.values())

            column_lineage: Dict[str, List[Dict[str, Any]]] = {}
            
            for i, proj in enumerate(select_exp.expressions):
                if isinstance(proj, exp.Star):
                    column_lineage["*"] = [{
                        "source_expression": "*",
                        "source_tables": immediate_sources,
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]
                    break
                
                tgt_col = proj.alias_or_name or f"col_{i+1}"
                
                # Skip invalid column names
                if not is_valid_column_name(tgt_col):
                    continue
                
                if getattr(proj, "this", None) is None:
                    continue
                
                src_sql = proj.this.sql(pretty=True).strip()
                
                # Handle both Column and Identifier (Identifier is used when column has implicit alias)
                if isinstance(proj.this, (exp.Column, exp.Identifier)):
                    table_alias, col_name = get_column_parts(proj.this)

                    # If col_name came back dotted, split into alias and column
                    if col_name and "." in col_name and table_alias is None:
                        table_alias, col_name = col_name.rsplit(".", 1)

                    if table_alias:
                        # Primary: alias lookup in source_map
                        source_table_name = source_map.get(table_alias)
                        # Fallback: if alias not found, assume the alias is already the table name
                        if not source_table_name:
                            source_table_name = table_alias
                    else:
                        # No alias on column; if only one source table is present, use it
                        source_table_name = immediate_sources[0] if len(immediate_sources) == 1 else None
                    # If still missing, try any single source in source_map
                    if not source_table_name and len(source_map.values()) == 1:
                        source_table_name = list(source_map.values())[0]
                    # If still missing, and source_map not empty, take the first mapped table
                    if not source_table_name and source_map:
                        source_table_name = next(iter(source_map.values()))
                    # Last resort: if still missing, use any immediate source (even if multiple)
                    if not source_table_name and immediate_sources:
                        source_table_name = immediate_sources[0]

                    # If column name is still missing, fall back to rendered SQL of the projection
                    if not col_name:
                        try:
                            col_name = proj.this.sql()
                        except Exception:
                            col_name = None
                        if col_name and "." in col_name:
                            # normalize dotted fallback
                            col_name = col_name.split(".")[-1]
                    # If both table and column are still missing, skip entry
                    if not source_table_name and not col_name:
                        continue
                    column_lineage[tgt_col] = [{
                        "source_table": source_table_name,
                        "source_column": col_name,
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]
                else:
                    column_lineage[tgt_col] = [{
                        "source_expression": src_sql,
                        "origin_map": dict(source_map),
                        "origin": target_table,
                        "subquery_cols": dict(subquery_cols_map)
                    }]

            base_sources: Set[str] = set()
            for s in immediate_sources:
                if s in table_defs:
                    base_sources.update(resolve_base_sources_local(s))
                else:
                    base_sources.add(s)

            table_defs[target_table] = {
                "column_lineage": column_lineage,
                "immediate_source_tables": immediate_sources,
                "source_tables": list(base_sources),
                "source_map": source_map,
                "subquery_cols_map": subquery_cols_map,
            }
        except Exception as e:
            print(f"Warning: Error processing statement: {e}")
            continue

    return table_defs


def resolve_to_base_columns(table_defs: Dict[str, Dict[str, Any]],
                            table_name: str,
                            col_name: str,
                            seen: Optional[Set[Tuple[str, str]]] = None) -> List[Tuple[str, str]]:
    """Resolve column through intermediate tables to base physical columns"""
    if seen is None:
        seen = set()
    key = (table_name, col_name)
    if key in seen:
        return []
    seen.add(key)

    if table_name not in table_defs:
        return [(table_name, col_name)]

    try:
        tdef = table_defs[table_name]
        col_lineage = tdef.get("column_lineage", {})
        entries = col_lineage.get(col_name, [])

        result: List[Tuple[str, str]] = []

        for ent in entries:
            if ent.get("source_column") and ent.get("source_table"):
                st = ent["source_table"]
                sc = ent["source_column"]
                if st in table_defs:
                    result.extend(resolve_to_base_columns(table_defs, st, sc, seen))
                else:
                    result.append((st, sc))
            elif ent.get("source_expression"):
                expr = ent["source_expression"]
                origin_map = ent.get("origin_map") or tdef.get("source_map", {})
                refs = extract_columns_from_expression(expr)
                for alias, cname in refs:
                    if not cname:
                        continue
                    if alias:
                        mapped = origin_map.get(alias) or alias
                    else:
                        vals = list(origin_map.values())
                        mapped = vals[0] if len(vals) == 1 else None
                        # Fallback: if no origin_map, and only one immediate_source, use it
                        if not mapped and tdef.get("immediate_source_tables"):
                            im = tdef.get("immediate_source_tables")
                            mapped = im[0] if len(im) == 1 else None
                    if mapped:
                        if mapped in table_defs:
                            result.extend(resolve_to_base_columns(table_defs, mapped, cname, seen))
                        else:
                            result.append((mapped, cname))

        if not result:
            for candidate, cand_def in table_defs.items():
                if candidate == table_name:
                    continue
                cand_cols = cand_def.get("column_lineage", {})
                if col_name in cand_cols:
                    cand_bases = resolve_to_base_columns(table_defs, candidate, col_name, seen)
                    if cand_bases:
                        result.extend(cand_bases)
                        break

        seen_pairs = set()
        dedup: List[Tuple[str, str]] = []
        for r in result:
            if r not in seen_pairs:
                seen_pairs.add(r)
                dedup.append(r)
        return dedup
    except Exception as e:
        print(f"Warning: Error in resolve_to_base_columns: {e}")
        return [(table_name, col_name)]


def expand_select_star_for_target(table_defs: Dict[str, Dict[str, Any]], target: str, col_rename_map: Optional[Dict[str, str]] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Expand SELECT * statements to individual columns"""
    if col_rename_map is None:
        col_rename_map = {}
    
    try:
        final = table_defs.get(target, {})
        tgt_col_lineage = final.get("column_lineage", {}) or {}
        immediate = final.get("immediate_source_tables", []) or []
        base_sources = final.get("source_tables", []) or []
        subquery_cols_map = final.get("subquery_cols_map", {}) or {}

        for col, entries in tgt_col_lineage.items():
            if col != "*":
                for ent in entries:
                    if ent.get("source_column"):
                        src_col = ent["source_column"]
                        col_rename_map[src_col] = col
                    subq_cols = ent.get("subquery_cols", {})
                    for sq_alias, col_pairs in subq_cols.items():
                        for src, tgt in col_pairs:
                            if tgt == col:
                                col_rename_map[src] = col

        if "*" in tgt_col_lineage or not tgt_col_lineage:
            source_candidates = immediate or base_sources
            if not source_candidates:
                return {}
            
            expanded: Dict[str, List[Dict[str, Any]]] = {}
            
            for source_table in source_candidates:
                if source_table not in table_defs:
                    continue
                
                src_cols = table_defs[source_table].get("column_lineage", {})
                src_subquery_map = table_defs[source_table].get("subquery_cols_map", {})
                
                for col, entries in src_cols.items():
                    if col == "*":
                        local_rename: Dict[str, str] = {}
                        for sq_alias, col_pairs in src_subquery_map.items():
                            for src_col, tgt_col in col_pairs:
                                local_rename[src_col] = tgt_col
                        
                        nested_expanded = expand_select_star_for_target(table_defs, source_table, local_rename)
                        for nested_col, nested_entries in nested_expanded.items():
                            final_col = col_rename_map.get(nested_col, nested_col)
                            if final_col not in expanded:
                                expanded[final_col] = nested_entries
                        continue
                    
                    final_col = col_rename_map.get(col, col)
                    final_entries: List[Dict[str, Any]] = []
                    
                    for ent in entries:
                        if ent.get("source_column") and ent.get("source_table"):
                            st = ent["source_table"]
                            sc = ent["source_column"]
                            if st in table_defs:
                                bases = resolve_to_base_columns(table_defs, st, sc)
                                for bt, bc in bases:
                                    final_entries.append({"source_table": bt, "source_column": bc, "origin": source_table})
                            else:
                                final_entries.append({"source_table": st, "source_column": sc, "origin": source_table})
                        else:
                            ent_copy = dict(ent)
                            ent_copy.setdefault("origin_map", table_defs.get(source_table, {}).get("source_map", {}))
                            ent_copy.setdefault("origin", source_table)
                            final_entries.append(ent_copy)
                    
                    expanded[final_col] = final_entries
            
            return expanded
        
        # Filter out invalid column names before returning
        filtered_lineage = {k: v for k, v in tgt_col_lineage.items() if is_valid_column_name(k)}
        return filtered_lineage
    except Exception as e:
        print(f"Warning: Error in expand_select_star_for_target: {e}")
        return {}


def detect_schema(table_defs: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """Guess a schema by looking at the first table name that contains a dot."""
    candidates: List[str] = []
    for tbl, tdef in table_defs.items():
        candidates.append(tbl)
        candidates.extend(tdef.get("source_tables", []))
        candidates.extend(tdef.get("immediate_source_tables", []))
    for name in candidates:
        if name and "." in name:
            return name.split(".", 1)[0]
    return None


def build_final_lineage(table_defs: Dict[str, Dict[str, Any]], target: str = None, default_schema: Optional[str] = None) -> Dict[str, Any]:
    """Build final column lineage output"""
    if not table_defs:
        return {"error": "No table definitions found"}
    
    # Auto-detect target if not provided
    actual_target = target
    if not actual_target:
        actual_target = list(table_defs.keys())[-1]
    elif actual_target not in table_defs:
        for key in table_defs.keys():
            if key.endswith(f".{actual_target}") or key == actual_target:
                actual_target = key
                break
        if actual_target not in table_defs:
            actual_target = list(table_defs.keys())[-1]

    try:
        tgt_def = table_defs[actual_target]
        final_map = expand_select_star_for_target(table_defs, actual_target) or tgt_def.get("column_lineage", {})

        schema_prefix = default_schema or detect_schema(table_defs) or "default"

        def add_schema_prefix(table_name: str) -> str:
            """Add schema prefix if not present"""
            if not table_name or "." in table_name:
                return table_name
            return f"{schema_prefix}.{table_name}"

        output_cols: Dict[str, List[Dict[str, Any]]] = {}
        
        for tgt_col, src_list in final_map.items():
            # Skip invalid target column names
            if not is_valid_column_name(tgt_col):
                continue
            
            source_cols_set: Set[str] = set()
            expr_set: Set[str] = set()
            is_transformation = False

            for src in src_list:
                if src.get("source_expression"):
                    expr_sql = src["source_expression"]
                    is_bare_column = (expr_sql == "*" or 
                                    (expr_sql and expr_sql.replace(".", "").replace("_", "").isalnum() and 
                                     not any(c in expr_sql for c in ["(", ")", ",", " "])))
                    
                    if not is_bare_column:
                        is_transformation = True
                        expr_set.add(expr_sql)
                    
                    origin_map = src.get("origin_map") or tgt_def.get("source_map", {})
                    for alias, cname in extract_columns_from_expression(expr_sql):
                        if not cname:
                            continue
                        if alias:
                            mapped = origin_map.get(alias) or alias
                        else:
                            vals = list(origin_map.values())
                            mapped = vals[0] if len(vals) == 1 else None
                        if mapped:
                            bases = resolve_to_base_columns(table_defs, mapped, cname)
                            if not bases:
                                for candidate, td in table_defs.items():
                                    if cname in td.get("column_lineage", {}):
                                        bases = resolve_to_base_columns(table_defs, candidate, cname)
                                        if bases:
                                            break
                            for bt, bc in bases:
                                if bt and bc and str(bt).upper() != "N/A" and is_valid_column_name(bc):
                                    bt_qualified = add_schema_prefix(bt)
                                    source_cols_set.add(f"{bt_qualified}.{bc}")
                    continue

                st = src.get("source_table")
                sc = src.get("source_column")
                if st and sc:
                    if st in table_defs:
                        bases = resolve_to_base_columns(table_defs, st, sc)
                        if not bases:
                            for candidate, td in table_defs.items():
                                if sc in td.get("column_lineage", {}):
                                    bases = resolve_to_base_columns(table_defs, candidate, sc)
                                    if bases:
                                        break
                        for bt, bc in bases:
                            if bt and bc and str(bt).upper() != "N/A" and is_valid_column_name(bc):
                                bt_qualified = add_schema_prefix(bt)
                                source_cols_set.add(f"{bt_qualified}.{bc}")
                    else:
                        if str(st).upper() != "N/A" and is_valid_column_name(sc):
                            st_qualified = add_schema_prefix(st)
                            source_cols_set.add(f"{st_qualified}.{sc}")

            output_entry = {
                "target_column": tgt_col,
                "source_columns": sorted(source_cols_set),
                "expressions": sorted(expr_set) if is_transformation else [],
            }
            output_cols[tgt_col] = [output_entry]

        source_tables_qualified = [add_schema_prefix(t) for t in tgt_def.get("source_tables", [])]

        cleaned = {
            "target_table": actual_target,
            "source_tables": sorted(source_tables_qualified),
        }
        cleaned.update(output_cols)
        return cleaned
    except Exception as e:
        print(f"Error in build_final_lineage: {e}")
        return {"error": str(e)}


def generate_lineage(sql: str, target_table: str = None, default_schema: Optional[str] = None) -> Dict[str, Any]:
    """
    Main function to generate column lineage from Hive SQL.
    
    Args:
        sql: Hive SQL statements (can be any valid Hive SQL)
        target_table: Name of the target table (if None, auto-detects last created table)
        default_schema: Default schema name to prepend to tables without schema
    
    Returns:
        Dictionary with column lineage information
    """
    expressions = parse_statements(sql)
    if not expressions:
        return {"error": "Failed to parse SQL statements"}
    
    table_defs = build_table_defs(expressions)
    if not table_defs:
        return {"error": "No table definitions found"}
    
    result = build_final_lineage(table_defs, target_table, default_schema)
    
    # Only use fallback parser for truly simple SQL (no CASE, no functions in SELECT)
    # Check if it's a simple CREATE TABLE AS SELECT
    is_simple_sql = ("case" not in sql.lower() and 
                     "concat" not in sql.lower() and
                     "replace" not in sql.lower() and
                     sql.count("create table") == 1)
    
    if is_simple_sql:
        # Check if source_columns are empty
        has_empty_sources = False
        for key, value in result.items():
            if key not in ["target_table", "source_tables", "error"]:
                if isinstance(value, list) and len(value) > 0:
                    if isinstance(value[0], dict) and "source_columns" in value[0]:
                        if not value[0]["source_columns"] or len(value[0]["source_columns"]) == 0:
                            has_empty_sources = True
                            break
        
        if has_empty_sources:
            print("Warning: Source columns empty, trying fallback parser...")
            fallback_result = parse_simple_create_table_as_select(sql)
            if fallback_result:
                # Build result from fallback parser
                enhanced_result = {
                    "target_table": fallback_result["target_table"],
                    "source_tables": [fallback_result["source_table"]]
                }
                
                # Map each target column to its source
                for src_col, tgt_col in zip(fallback_result["source_columns"], fallback_result["target_columns"]):
                    col_name = tgt_col["column"]
                    enhanced_result[col_name] = [{
                        "target_column": col_name,
                        "source_columns": [f"{src_col['table']}.{src_col['column']}"],
                        "expressions": []
                    }]
                
                return enhanced_result
    
    return result


def generate_multi_hop_lineage(table_defs: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate multi-hop lineage data with intermediate tables.
    Creates a graph structure showing source → intermediate → target flow.
    """
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    node_ids = set()
    
    # Process all tables in the definition
    processed_tables = set()
    
    for table_name, table_def in table_defs.items():
        processed_tables.add(table_name)
        
        # Determine table type
        is_target = (table_name == "derived_df" or table_name.endswith("_df"))
        is_intermediate = not is_target and table_def.get("immediate_source_tables")
        
        # Add table node
        table_node = {
            "id": table_name,
            "label": table_name,
            "type": "table",
            "parent_id": None
        }
        if table_node["id"] not in node_ids:
            nodes.append(table_node)
            node_ids.add(table_node["id"])
        
        # Add column nodes for this table
        col_lineage = table_def.get("column_lineage", {})
        for col_name in col_lineage.keys():
            if col_name == "*":
                continue
            if not is_valid_column_name(col_name):
                continue
            
            col_id = f"{table_name}.{col_name}"
            col_node = {
                "id": col_id,
                "label": col_name,
                "type": "column",
                "parent_id": table_name
            }
            if col_node["id"] not in node_ids:
                nodes.append(col_node)
                node_ids.add(col_node["id"])
    
    # Add base source tables (those not defined in table_defs)
    all_source_tables = set()
    for table_def in table_defs.values():
        all_source_tables.update(table_def.get("source_tables", []))
    
    for src_table in all_source_tables:
        if src_table not in processed_tables:
            table_node = {
                "id": src_table,
                "label": src_table,
                "type": "table",
                "parent_id": None
            }
            if table_node["id"] not in node_ids:
                nodes.append(table_node)
                node_ids.add(table_node["id"])
    
    # Create table-level edges
    for table_name, table_def in table_defs.items():
        for src_table in table_def.get("immediate_source_tables", []):
            edge = {
                "source": src_table,
                "target": table_name,
                "flow_type": "table_level"
            }
            edges.append(edge)
    
    # Create column-level edges by resolving lineage
    for target_table, table_def in table_defs.items():
        col_lineage = table_def.get("column_lineage", {})
        
        for tgt_col, src_list in col_lineage.items():
            if tgt_col == "*" or not is_valid_column_name(tgt_col):
                continue
            
            target_col_id = f"{target_table}.{tgt_col}"
            
            for src_entry in src_list:
                # Handle direct column references
                if src_entry.get("source_table") and src_entry.get("source_column"):
                    src_table = src_entry["source_table"]
                    src_col = src_entry["source_column"]
                    
                    # If source is an intermediate table, trace to base
                    if src_table in table_defs:
                        bases = resolve_to_base_columns(table_defs, src_table, src_col)
                        for base_table, base_col in bases:
                            if base_col and is_valid_column_name(base_col):
                                source_col_id = f"{base_table}.{base_col}"
                                edge = {
                                    "source": source_col_id,
                                    "target": target_col_id,
                                    "flow_type": "column_level",
                                    "logic": "Direct Pass-Through"
                                }
                                edges.append(edge)
                    else:
                        # Direct base table reference
                        if src_col and is_valid_column_name(src_col):
                            source_col_id = f"{src_table}.{src_col}"
                            edge = {
                                "source": source_col_id,
                                "target": target_col_id,
                                "flow_type": "column_level",
                                "logic": "Direct Pass-Through"
                            }
                            edges.append(edge)
                
                # Handle expressions
                elif src_entry.get("source_expression"):
                    expr = src_entry["source_expression"]
                    if expr != "*":
                        origin_map = src_entry.get("origin_map") or table_def.get("source_map", {})
                        refs = extract_columns_from_expression(expr)
                        
                        for alias, col_name in refs:
                            if not col_name or not is_valid_column_name(col_name):
                                continue
                            
                            if alias:
                                src_table = origin_map.get(alias) or alias
                            else:
                                vals = list(origin_map.values())
                                src_table = vals[0] if len(vals) == 1 else None
                            
                            if src_table:
                                if src_table in table_defs:
                                    bases = resolve_to_base_columns(table_defs, src_table, col_name)
                                    for base_table, base_col in bases:
                                        if base_col and is_valid_column_name(base_col):
                                            source_col_id = f"{base_table}.{base_col}"
                                            edge = {
                                                "source": source_col_id,
                                                "target": target_col_id,
                                                "flow_type": "column_level",
                                                "logic": expr[:150]  # Truncate long expressions
                                            }
                                            edges.append(edge)
                                else:
                                    if col_name and is_valid_column_name(col_name):
                                        source_col_id = f"{src_table}.{col_name}"
                                        edge = {
                                            "source": source_col_id,
                                            "target": target_col_id,
                                            "flow_type": "column_level",
                                            "logic": expr[:150]
                                        }
                                        edges.append(edge)
    
    # Remove duplicate edges
    unique_edges = []
    seen_edges = set()
    for edge in edges:
        edge_key = (edge["source"], edge["target"], edge.get("logic", ""))
        if edge_key not in seen_edges:
            unique_edges.append(edge)
            seen_edges.add(edge_key)
    
    return {
        "nodes": nodes,
        "edges": unique_edges
    }


def extract_column_references(expression_text: str) -> Set[str]:
    """
    Extract column references from SQL expression text.
    Returns a set of column names referenced in the expression.
    Captures both raw.column_name and derv.column_name patterns.
    """
    if not expression_text:
        return set()
    
    column_refs = set()
    import re
    
    # Match patterns: raw.column_name or derv.column_name 
    pattern = r'\b(?:raw|derv)\.([a-zA-Z_][a-zA-Z0-9_]*)\b'
    
    matches = re.findall(pattern, expression_text, re.IGNORECASE)
    for match in matches:
        column_name = match
        if column_name and column_name.upper() not in ('YEAR', 'MONTH', 'DAY', 'CAST', 'DATE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'IN', 'AND', 'OR', 'NOT'):
            column_refs.add(column_name)
    
    return column_refs


def validate_and_fix_lineage(lineage_data: Dict[str, Any], source_tables: List[str]) -> Tuple[Dict[str, Any], List[str]]:
    """
    Validate lineage data and fix missing source columns.
    Automatically detects columns referenced in expressions and adds them to source_columns if missing.
    Maps derv_* references to their base column names since derv_* are derived/calculated columns.
    Removes duplicate source columns.
    
    Returns:
        Tuple of (fixed_lineage_data, list_of_fixed_issues)
    """
    fixed_issues = []
    reserved_keys = {'target_table', 'source_tables'}
    
    for target_col, definitions in lineage_data.items():
        if target_col in reserved_keys:
            continue
        
        if not isinstance(definitions, list):
            continue
        
        for definition in definitions:
            if not isinstance(definition, dict):
                continue
            
            # Get existing source columns
            existing_sources = definition.get('source_columns', [])
            
            # Extract columns from expressions to know what's actually used
            expressions = definition.get('expressions', [])
            referenced_columns = set()
            
            for expr in expressions:
                if expr:
                    refs = extract_column_references(expr)
                    referenced_columns.update(refs)
            
            # Map derv_* columns to their base column names
            # derv_state -> state, derv_u_field -> u_field, etc.
            normalized_references = set()
            for col_ref in referenced_columns:
                col_ref_lower = col_ref.lower()
                # Strip derv_ prefix if present to get the base column
                if col_ref_lower.startswith('derv_'):
                    base_col = col_ref_lower[5:]  # Remove 'derv_' prefix
                    normalized_references.add(base_col)
                else:
                    normalized_references.add(col_ref_lower)
            
            # First pass: deduplicate and filter to only used columns
            seen = set()
            deduplicated_sources = []
            duplicates_removed = []
            unused_sources = []
            
            for col in existing_sources:
                col_parts = col.split('.')
                col_name = col_parts[-1] if col_parts else col
                col_name_lower = col_name.lower()
                
                # Strip derv_ prefix from stored column if present
                base_col_name = col_name_lower[5:] if col_name_lower.startswith('derv_') else col_name_lower
                
                # Check if this column (or its base form) is referenced in expressions
                is_referenced = base_col_name in normalized_references
                
                if not is_referenced:
                    # Column not referenced at all
                    unused_sources.append(col)
                    continue
                
                # This column is referenced, keep it (deduplicate)
                if col not in seen:
                    seen.add(col)
                    deduplicated_sources.append(col)
                else:
                    if col not in duplicates_removed:
                        duplicates_removed.append(col)
            
            if duplicates_removed:
                fixed_issues.append(f"  {target_col}: Removed duplicate source columns: {', '.join(sorted(duplicates_removed))}")
            
            if unused_sources:
                fixed_issues.append(f"  {target_col}: Removed unused source columns: {', '.join(sorted(unused_sources))}")
            
            # Find missing columns (columns in expressions but not in source_columns)
            # Start with normalized references
            missing_columns = set(normalized_references)
            
            # Remove any that are already in deduplicated_sources
            for existing in deduplicated_sources:
                existing_parts = existing.split('.')
                existing_col = existing_parts[-1] if existing_parts else existing
                existing_col_lower = existing_col.lower()
                # Strip derv_ prefix for comparison
                existing_base = existing_col_lower[5:] if existing_col_lower.startswith('derv_') else existing_col_lower
                missing_columns.discard(existing_base)
            
            # For missing columns, try to find them in any raw source table
            missing_columns_full_path = set()
            for col in missing_columns:
                # Try to match with existing source columns to get the right table
                for existing in deduplicated_sources:
                    existing_parts = existing.split('.')
                    if len(existing_parts) >= 3:
                        existing_table = '.'.join(existing_parts[:-1])
                        # If this is a raw table, use it for the missing column
                        if 'raw' in existing_table.lower():
                            full_col = f"{existing_table}.{col}"
                            if full_col not in deduplicated_sources:
                                missing_columns_full_path.add(full_col)
                            break
            
            # If no matching table found, default to orc_issue_mgmt_raw.issues
            if not missing_columns_full_path and missing_columns:
                for col in missing_columns:
                    full_col = f"orc_issue_mgmt_raw.issues.{col}"
                    if full_col not in deduplicated_sources:
                        missing_columns_full_path.add(full_col)
            
            # Update source_columns with deduplicated and added columns
            new_sources = deduplicated_sources + sorted(list(missing_columns_full_path))
            if new_sources != existing_sources:
                definition['source_columns'] = new_sources
            
            if missing_columns_full_path:
                fixed_issues.append(f"  {target_col}: Added missing columns: {', '.join(sorted(missing_columns_full_path))}")
    
    return lineage_data, fixed_issues


def main():
    # Generate direct column lineage
    result = generate_lineage(SQL_STATEMENTS, default_schema=None)
    
    # Validate and fix missing source columns
    source_tables = result.get('source_tables', [])
    result, fixed_issues = validate_and_fix_lineage(result, source_tables)
    
    if fixed_issues:
        print("\n=== Fixed Missing Source Columns ===")
        for issue in fixed_issues:
            print(issue)
    
    with open("C:\\Users\\ADMIN\\Desktop\\AI\\hack\\modify_data.json", "w") as f:
        json.dump(result, f, indent=4)
    
    print("\nDirect column lineage written to modify_data.json")
    
    # Generate multi-hop lineage with intermediate tables
    expressions = parse_statements(SQL_STATEMENTS)
    if expressions:
        table_defs = build_table_defs(expressions)
        if table_defs:
            multi_hop_result = generate_multi_hop_lineage(table_defs)
            
            with open("C:\\Users\\ADMIN\\Desktop\\AI\\hack\\lineage_data.json", "w") as f:
                json.dump(multi_hop_result, f, indent=4)
            
            print("Multi-hop lineage with intermediate tables written to lineage_data.json")
        else:
            print("Warning: Could not build table definitions for multi-hop lineage")
    else:
        print("Warning: Could not parse SQL statements for multi-hop lineage")


if __name__ == "__main__":
    main()
