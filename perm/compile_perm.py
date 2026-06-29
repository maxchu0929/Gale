import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(PROJECT_ROOT)
BASE_PATH    = os.path.join(REPO_ROOT, "data", "PERM Program")

print("Using PERM root:", BASE_PATH)

# ---------------------------------------------------------
# 1. FINAL SCHEMA (your canonical unified column set)
# ---------------------------------------------------------
FINAL_SCHEMA = [

    # -----------------------------
    # SECTION A — CASE INFO
    # -----------------------------
    "case_number",
    "case_status",
    "received_date",
    "decision_date",
    "refile",
    "orig_file_date",
    "previous_swa_case_number_state",
    "schd_a_sheepherder",
    "occupation_type",

    # -----------------------------
    # SECTION B — EMPLOYER (canonical = NEW names)
    # -----------------------------
    "emp_business_name",
    "emp_trade_name",
    "emp_addr1",
    "emp_addr2",
    "emp_city",
    "emp_state",
    "emp_postcode",
    "emp_country",
    "emp_province",
    "emp_phone",
    "emp_phoneext",
    "emp_num_payroll",
    "emp_year_commenced",
    "emp_fein",
    "emp_naics",
    "emp_worker_interest",
    "emp_relationship_worker",

    # -----------------------------
    # EMPLOYER POC (canonical = NEW names)
    # -----------------------------
    "emp_poc_last_name",
    "emp_poc_first_name",
    "emp_poc_middle_name",
    "emp_poc_job_title",
    "emp_poc_addr1",
    "emp_poc_addr2",
    "emp_poc_city",
    "emp_poc_state",
    "emp_poc_postal_code",
    "emp_poc_country",
    "emp_poc_province",
    "emp_poc_phone",
    "emp_poc_phoneext",
    "emp_poc_email",

    # -----------------------------
    # OLD POC fields (mapped → POC canonical where possible)
    # -----------------------------
    "emp_contact_name",
    "emp_contact_phone",
    "emp_contact_email",

    # -----------------------------
    # ATTORNEY / AGENT
    # -----------------------------
    "atty_ag_rep_type",
    "atty_ag_last_name",
    "atty_ag_first_name",
    "atty_ag_middle_name",
    "atty_ag_address1",
    "atty_ag_address2",
    "atty_ag_city",
    "atty_ag_state",
    "atty_ag_postal_code",
    "atty_ag_country",
    "atty_ag_province",
    "atty_ag_phone",
    "atty_ag_phone_ext",
    "atty_ag_email",
    "atty_ag_law_firm_name",
    "atty_ag_fein",
    "atty_ag_state_bar_number",
    "atty_ag_good_standing_state",
    "atty_ag_good_standing_court",

    # -----------------------------
    # PREVAILING WAGE (OLD & NEW)
    # -----------------------------
    "pw_track_number",
    "pw_soc_code",
    "pw_soc_title",
    "pw_skill_level",
    "pw_wage",
    "pw_unit_of_pay",
    "pw_wage_source",
    "pw_source_name_other",
    "pw_determination_date",
    "pw_expiration_date",

    # NEW-form equivalent
    "job_opp_pwd_number",
    "job_opp_pwd_attached",
    "job_opp_wage_from",
    "job_opp_wage_to",
    "job_opp_wage_per",
    "job_opp_wage_conditions",

    # -----------------------------
    # WAGE OFFER (old)
    # -----------------------------
    "wage_offer_from",
    "wage_offer_to",
    "wage_offer_unit_of_pay",

    # -----------------------------
    # CONDITIONAL REQUIREMENT FLAGS (new)
    # -----------------------------
    "other_req_is_fulltime_emp",
    "other_req_is_livein_household",
    "other_req_is_paid_experience",
    "other_req_is_fw_executed_cont",
    "other_req_is_emp_provided_cont",
    "other_req_accept_diploma_pwd",
    "other_req_is_fw_currently_wrk",
    "other_req_is_fw_qualify",
    "other_req_emp_will_accept",
    "other_req_emp_rely_exp",
    "other_req_fw_gain_exp",
    "other_req_emp_pay_education",
    "other_req_job_emp_premises",
    "other_req_job_combo_occup",
    "other_req_job_foreign_language",
    "other_req_job_req_exceed",
    "other_req_emp_use_credential",
    "other_req_emp_rec_payment",
    "other_req_emp_layoff",

    # -----------------------------
    # WORKSITE
    # -----------------------------
    "primary_worksite_type",
    "primary_worksite_addr1",
    "primary_worksite_addr2",
    "primary_worksite_city",
    "primary_worksite_county",
    "primary_worksite_state",
    "primary_worksite_postal_code",
    "primary_worksite_bls_area",
    "is_multiple_locations",
    "is_appendix_b_attached",

    # OLD worksite names (mapped)
    "worksite_address_1",
    "worksite_address_2",
    "worksite_city",
    "worksite_state",
    "worksite_postal_code",

    # -----------------------------
    # JOB REQUIREMENTS
    # -----------------------------
    "job_title",
    "minimum_education",
    "job_education_min_other",
    "major_field_of_study",
    "required_training",
    "required_training_months",
    "required_field_of_training",
    "required_experience",
    "required_experience_months",
    "accept_alt_field_of_study",
    "accept_alt_major_fld_of_study",
    "accept_alt_combo",
    "accept_alt_combo_education",
    "accept_alt_combo_ed_other",
    "accept_alt_combo_education_yrs",
    "accept_foreign_education",
    "accept_alt_occupation",
    "accept_alt_occupation_months",
    "accept_alt_job_title",
    "job_opp_requirements_normal",
    "foreign_language_required",
    "specific_skills",
    "combination_occupation",
    "offered_to_appl_foreign_worker",

    # -----------------------------
    # FOREIGN WORKER
    # -----------------------------
    "professional_occupation",
    "app_for_college_u_teacher",
    "competitive_process",
    "basic_recruitment_process",
    "teacher_select_date",
    "teacher_pub_journal_name",
    "add_recruit_information",

    "country_of_citizenship",
    "foreign_worker_birth_country",
    "class_of_admission",
    "foreign_worker_education",
    "foreign_worker_education_other",
    "foreign_worker_info_major",
    "foreign_worker_yrs_ed_comp",
    "foreign_worker_inst_of_ed",
    "foreign_worker_ed_inst_add_1",
    "foreign_worker_ed_inst_add_2",
    "foreign_worker_ed_inst_city",
    "foreign_worker_ed_inst_state_p",
    "foreign_worker_ed_inst_country",
    "foreign_worker_ed_inst_post_cd",
    "foreign_worker_training_comp",
    "foreign_worker_req_experience",
    "foreign_worker_alt_ed_exp",
    "foreign_worker_alt_occ_exp",
    "foreign_worker_exp_with_empl",
    "foreign_worker_empl_pay_for_ed",
    "foreign_worker_curr_employed",
    "foreign_worker_live_on_prem",
    "foreign_worker_live_in_dom_ser",
    "foreign_worker_live_in_dom_svc_cnt",

    # -----------------------------
    # RECRUITMENT
    # -----------------------------
    "swa_job_order_start_date",
    "swa_job_order_end_date",
    "sunday_edition_newspaper",
    "first_newspaper_name",
    "first_advertisement_start_date",
    "second_newspaper_ad_name",
    "second_advertisement_type",
    "second_ad_start_date",
    "job_fair_from_date",
    "job_fair_to_date",
    "on_campus_recruiting_from_date",
    "on_campus_recruiting_to_date",
    "employer_website_from_date",
    "employer_website_to_date",
    "pro_org_ad_from_date",
    "pro_org_advertisement_to_date",
    "job_search_website_from_date",
    "job_search_website_to_date",
    "pvt_employment_firm_from_date",
    "pvt_employment_firm_to_date",
    "employee_ref_prog_from_date",
    "employee_referral_prog_to_date",
    "campus_placement_from_date",
    "campus_placement_to_date",
    "local_ethnic_paper_from_date",
    "local_ethnic_paper_to_date",
    "radio_tv_ad_from_date",
    "radio_tv_ad_to_date",

    # NEW recruitment fields
    "recr_info_recruit_supervised_req",
    "recr_info_job_start_date",
    "recr_info_job_end_date",
    "recr_info_is_newspaper_sunday",
    "recr_info_newspaper_name",
    "recr_info_ad_date1",
    "recr_info_recruit_ad_type",
    "recr_info_newspaper_name2",
    "recr_info_ad_date2",
    "recr_occ_job_fair_from",
    "recr_occ_job_fair_to",
    "recr_occ_emp_website_from",
    "recr_occ_emp_website_to",
    "recr_occ_job_search_from",
    "recr_occ_job_search_to",
    "recr_occ_on_campus_from",
    "recr_occ_on_campus_to",
    "recr_occ_trade_org_from",
    "recr_occ_trade_org_to",
    "recr_occ_private_emp_from",
    "recr_occ_private_emp_to",
    "recr_occ_emp_referral_from",
    "recr_occ_emp_referral_to",
    "recr_occ_campus_placement_from",
    "recr_occ_campus_placement_to",
    "recr_occ_local_newspaper_from",
    "recr_occ_local_newspaper_to",
    "recr_occ_radio_ad_from",
    "recr_occ_radio_ad_to",

    # -----------------------------
    # NOTICE & COMPLIANCE
    # -----------------------------
    "emp_received_payment",
    "payment_details",
    "bargaining_rep_notified",
    "posted_notice_at_worksite",
    "layoff_in_past_six_months",
    "us_workers_considered",

    "notice_post_bargain_rep",
    "notice_post_bargain_rep_physical",
    "notice_post_bargain_rep_electronic",
    "notice_post_bargain_rep_inhouse",
    "notice_post_bargain_rep_private",
    "notice_post_emp_not_posted",
    "emp_certify_compliance",

    # -----------------------------
    # PREPARER
    # -----------------------------
    "employer_completed_application",
    "preparer_name",
    "preparer_title",
    "preparer_email",

    "decl_prep_last_name",
    "decl_prep_first_name",
    "decl_prep_middle_name",
    "decl_prep_lawfirm_fein",
    "decl_prep_firm_business_name",
    "decl_prep_email",

    # -----------------------------
    # INTERNAL METADATA
    # -----------------------------
    "year",
    "form_type",
]


# ---------------------------------------------------------
# 2. ALIAS MAPPING (old names → canonical)
#    You can expand this as needed.
# ---------------------------------------------------------
ALIAS_MAP = {
    # ---------------------------------------------
    # EMPLOYER NAME / BUSINESS NAME
    # ---------------------------------------------
    "employer_name": "employer_name",
    "emp_business_name": "employer_name",

    # ---------------------------------------------
    # EMPLOYER ADDRESS (OLD → CANONICAL)
    # ---------------------------------------------
    "employer_address_1": "employer_addr1",
    "employer_address_2": "employer_addr2",
    "employer_city": "employer_city",
    "employer_state_province": "employer_state_province",
    "employer_postal_code": "employer_postal_code",
    "employer_country": "employer_country",
    "employer_phone": "employer_phone",
    "employer_phone_ext": "employer_phone_ext",
    "employer_num_employees": "employer_num_employees",
    "employer_year_commenced_business": "employer_year_commenced_business",
    "employer_fein": "employer_fein",

    # ---------------------------------------------
    # EMPLOYER ADDRESS (NEW → CANONICAL)
    # ---------------------------------------------
    "emp_addr1": "employer_addr1",
    "emp_addr2": "employer_addr2",
    "emp_city": "employer_city",
    "emp_state": "employer_state_province",
    "emp_postcode": "employer_postal_code",
    "emp_country": "employer_country",
    "emp_phone": "employer_phone",
    "emp_phoneext": "employer_phone_ext",
    "emp_fein": "employer_fein",

    # ---------------------------------------------
    # INDUSTRY CODES
    # ---------------------------------------------
    "naics_code": "naics_code",
    "emp_naics": "naics_code",

    # ---------------------------------------------
    # PAYROLL SIZE
    # ---------------------------------------------
    "emp_num_payroll": "employer_num_employees",

    # ---------------------------------------------
    # EMPLOYER RELATIONSHIP / OWNERSHIP
    # ---------------------------------------------
    "fw_ownership_interest": "fw_ownership_interest",
    "emp_worker_interest": "fw_ownership_interest",  # new-form version
    "emp_relationship_worker": "emp_relationship_worker",

    # ---------------------------------------------
    # CONTACT PERSON (OLD-FORM)
    # ---------------------------------------------
    "emp_contact_name": "emp_contact_name",
    "emp_contact_address_1": "emp_contact_address_1",
    "emp_contact_address_2": "emp_contact_address_2",
    "emp_contact_city": "emp_contact_city",
    "emp_contact_state_province": "emp_contact_state_province",
    "emp_contact_country": "emp_contact_country",
    "emp_contact_postal_code": "emp_contact_postal_code",
    "emp_contact_phone": "emp_contact_phone",
    "emp_contact_email": "emp_contact_email",

    # ---------------------------------------------
    # CONTACT PERSON (NEW-FORM)
    # ---------------------------------------------
    "emp_poc_last_name": "emp_poc_last_name",
    "emp_poc_first_name": "emp_poc_first_name",
    "emp_poc_middle_name": "emp_poc_middle_name",
    "emp_poc_job_title": "emp_poc_job_title",
    "emp_poc_addr1": "emp_poc_addr1",
    "emp_poc_addr2": "emp_poc_addr2",
    "emp_poc_city": "emp_poc_city",
    "emp_poc_state": "emp_poc_state",
    "emp_poc_postal_code": "emp_poc_postal_code",
    "emp_poc_country": "emp_poc_country",
    "emp_poc_province": "emp_poc_province",
    "emp_poc_phone": "emp_poc_phone",
    "emp_poc_phoneext": "emp_poc_phoneext",
    "emp_poc_email": "emp_poc_email",

    # ---------------------------------------------
    # ATTORNEY / AGENT (OLD-FORM)
    # ---------------------------------------------
    "agent_attorney_name": "agent_attorney_name",
    "agent_attorney_firm_name": "agent_attorney_firm_name",
    "agent_attorney_phone": "agent_attorney_phone",
    "agent_attorney_phone_ext": "agent_attorney_phone_ext",
    "agent_attorney_address_1": "agent_attorney_address_1",
    "agent_attorney_address_2": "agent_attorney_address_2",
    "agent_attorney_city": "agent_attorney_city",
    "agent_attorney_state_province": "agent_attorney_state_province",
    "agent_attorney_country": "agent_attorney_country",
    "agent_attorney_postal_code": "agent_attorney_postal_code",
    "agent_attorney_email": "agent_attorney_email",

    # ---------------------------------------------
    # ATTORNEY / AGENT (NEW-FORM)
    # ---------------------------------------------
    "atty_ag_rep_type": "atty_ag_rep_type",
    "atty_ag_last_name": "atty_ag_last_name",
    "atty_ag_first_name": "atty_ag_first_name",
    "atty_ag_middle_name": "atty_ag_middle_name",
    "atty_ag_address1": "atty_ag_address1",
    "atty_ag_address2": "atty_ag_address2",
    "atty_ag_city": "atty_ag_city",
    "atty_ag_state": "atty_ag_state",
    "atty_ag_postal_code": "atty_ag_postal_code",
    "atty_ag_country": "atty_ag_country",
    "atty_ag_province": "atty_ag_province",
    "atty_ag_phone": "atty_ag_phone",
    "atty_ag_phone_ext": "atty_ag_phone_ext",
    "atty_ag_email": "atty_ag_email",
    "atty_ag_law_firm_name": "atty_ag_law_firm_name",
    "atty_ag_fein": "atty_ag_fein",
    "atty_ag_state_bar_number": "atty_ag_state_bar_number",
    "atty_ag_good_standing_state": "atty_ag_good_standing_state",
    "atty_ag_good_standing_court": "atty_ag_good_standing_court",

    # ---------------------------------------------
    # PREVAILING WAGE (PWD)
    # ---------------------------------------------
    "pw_track_number": "pw_track_number",
    "pw_soc_code": "pw_soc_code",
    "pw_soc_title": "pw_soc_title",
    "pw_skill_level": "pw_skill_level",
    "pw_wage": "pw_wage",
    "pw_unit_of_pay": "pw_unit_of_pay",
    "pw_wage_source": "pw_wage_source",
    "pw_source_name_other": "pw_source_name_other",
    "pw_determination_date": "pw_determination_date",
    "pw_expiration_date": "pw_expiration_date",

    # NEW-FORM PWD equivalents
    "job_opp_pwd_number": "pw_track_number",
    "job_opp_pwd_attached": "job_opp_pwd_attached",
    "job_opp_wage_from": "job_opp_wage_from",
    "job_opp_wage_to": "job_opp_wage_to",
    "job_opp_wage_per": "job_opp_wage_per",
    "job_opp_wage_conditions": "job_opp_wage_conditions",

    # ---------------------------------------------
    # WAGE OFFER (OLD)
    # ---------------------------------------------
    "wage_offer_from": "wage_offer_from",
    "wage_offer_to": "wage_offer_to",
    "wage_offer_unit_of_pay": "wage_offer_unit_of_pay",

    # ---------------------------------------------
    # WORKSITE (OLD)
    # ---------------------------------------------
    "worksite_address_1": "worksite_address_1",
    "worksite_address_2": "worksite_address_2",
    "worksite_city": "worksite_city",
    "worksite_state": "worksite_state",
    "worksite_postal_code": "worksite_postal_code",

    # ---------------------------------------------
    # WORKSITE (NEW)
    # ---------------------------------------------
    "primary_worksite_addr1": "primary_worksite_addr1",
    "primary_worksite_addr2": "primary_worksite_addr2",
    "primary_worksite_city": "primary_worksite_city",
    "primary_worksite_state": "primary_worksite_state",
    "primary_worksite_postal_code": "primary_worksite_postal_code",
    "primary_worksite_county": "primary_worksite_county",
    "primary_worksite_bls_area": "primary_worksite_bls_area",
    "primary_worksite_type": "primary_worksite_type",
    "is_multiple_locations": "is_multiple_locations",
    "is_appendix_b_attached": "is_appendix_b_attached",
}


# ---------------------------------------------------------
# 3. Normalize column names
# ---------------------------------------------------------
def normalize_columns(cols):
    cols = (
        cols.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )
    cols = cols.str.replace(r"^unnamed.*", "", regex=True)
    return cols

# ---------------------------------------------------------
# 4. Detect form type
# ---------------------------------------------------------
def detect_form_type(filename, year):
    f = filename.lower()
    if "old_form" in f:
        return "old"
    if int(year) <= 2023:
        return "old"
    return "new"

# ---------------------------------------------------------
# 5. Load parquet file
# ---------------------------------------------------------
def load_perm_file(file_path):
    pq = file_path.replace(".xlsx", ".parquet")
    if os.path.exists(pq):
        print(" → Loading parquet:", pq)
        return pd.read_parquet(pq)
    print(" ⚠️ Skipping (no parquet):", file_path)
    return None

# ---------------------------------------------------------
# 6. Clean + map alias → canonical
# ---------------------------------------------------------
def clean_and_map(df, year, form_type):

    df = df.copy()

    # Remove garbage columns
    df = df.loc[:, ~df.columns.duplicated()]
    df = df.dropna(axis=1, how="all")
    df = df[[c for c in df.columns if c]]

    # Apply alias mapping
    for old, new in ALIAS_MAP.items():
        if old in df.columns:
            df[new] = df[old]
            df = df.drop(columns=[old], errors="ignore")

    # Add year + form_type
    df["year"] = year
    df["form_type"] = form_type

    return df

# ---------------------------------------------------------
# 7. Reindex to FINAL_SCHEMA
# ---------------------------------------------------------
def enforce_final_schema(df):
    # Find missing canonical columns
    missing = [c for c in FINAL_SCHEMA if c not in df.columns]

    # Create a block of missing columns all at once
    if missing:
        df = pd.concat([df, pd.DataFrame({c: [None] * len(df) for c in missing})], axis=1)

    # Reorder exactly to FINAL_SCHEMA
    df = df[FINAL_SCHEMA]

    # Defragment automatically
    df = df.copy()
    return df

# ---------------------------------------------------------
# 8. Main compiler
# ---------------------------------------------------------
def compile_perm():

    all_rows = []

    for year in sorted(os.listdir(BASE_PATH)):
        year_path = os.path.join(BASE_PATH, year)
        if not os.path.isdir(year_path):
            continue

        for fname in os.listdir(year_path):
            if not fname.endswith(".xlsx"):
                continue

            full_path = os.path.join(year_path, fname)
            print("\nLoading:", full_path)

            form_type = detect_form_type(fname, year)

            df = load_perm_file(full_path)
            if df is None:
                continue

            df.columns = normalize_columns(df.columns)
            df = clean_and_map(df, year, form_type)
            df = enforce_final_schema(df)

            all_rows.append(df)

    final = pd.concat(all_rows, ignore_index=True)

    # Deduplicate by case_number
    if "case_number" in final.columns:
        final["case_number"] = final["case_number"].astype(str).str.upper()
        final = final.drop_duplicates(subset=["case_number"])

    # Output CSV
    outpath = os.path.join(PROJECT_ROOT, "perm_db.csv")
    final.to_csv(outpath, index=False)

    print("\n--------------------------------------------------")
    print("Saved unified PERM dataset to:", outpath)
    print("Rows:", len(final))
    print("Columns:", len(final.columns))
    print("--------------------------------------------------")


if __name__ == "__main__":
    compile_perm()
