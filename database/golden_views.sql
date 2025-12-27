-- Golden Views for SAGE Clinical Platform
-- Pre-joined views that simplify complex queries and improve accuracy
--
-- These views are referenced by the GoldenViewRouter to route complex
-- multi-table queries to simpler, validated SQL patterns.
--
-- Usage: Run this script against the clinical DuckDB database to create views
-- Command: duckdb clinical.duckdb < golden_views.sql

-- ============================================================================
-- VIEW 1: vw_ae_with_demographics
-- Purpose: Adverse events joined with subject demographics
-- Use for: AE analysis by demographic characteristics (age, sex, race, treatment)
-- ============================================================================
CREATE OR REPLACE VIEW vw_ae_with_demographics AS
SELECT
    -- Subject identifiers
    ae.STUDYID,
    ae.USUBJID,
    ae.SUBJID,

    -- Demographics
    dm.AGE,
    dm.AGEU,
    dm.SEX,
    dm.RACE,
    dm.ETHNIC,
    dm.COUNTRY,
    dm.SITEID,

    -- Treatment (from ADSL if available, else DM)
    COALESCE(adsl.TRT01P, dm.ARM) AS TREATMENT,
    COALESCE(adsl.TRT01PN, dm.ARMCD) AS TREATMENT_N,

    -- Adverse Event details
    ae.AESEQ,
    ae.AESPID,
    ae.AETERM,
    ae.AEDECOD,
    ae.AEBODSYS,
    ae.AESOC,
    ae.AESEV,
    ae.AESER,
    ae.AEREL,
    ae.AEACN,
    ae.AEOUT,
    ae.AESTDTC,
    ae.AEENDTC,
    ae.AESTDY,
    ae.AEENDY,

    -- ADaM AE extensions (if ADAE exists)
    adae.AETOXGR,
    adae.AETRTEM,
    adae.TRTEMFL,

    -- Computed fields
    CASE
        WHEN ae.AESER = 'Y' THEN 'Serious'
        ELSE 'Non-Serious'
    END AS AE_SERIOUSNESS,

    CASE
        WHEN ae.AEREL IN ('PROBABLE', 'POSSIBLE', 'DEFINITE') THEN 'Related'
        WHEN ae.AEREL IN ('NOT RELATED', 'UNLIKELY') THEN 'Not Related'
        ELSE 'Unknown'
    END AS AE_RELATEDNESS

FROM ae
LEFT JOIN dm ON ae.USUBJID = dm.USUBJID AND ae.STUDYID = dm.STUDYID
LEFT JOIN adsl ON ae.USUBJID = adsl.USUBJID AND ae.STUDYID = adsl.STUDYID
LEFT JOIN adae ON ae.USUBJID = adae.USUBJID AND ae.AESEQ = adae.AESEQ;


-- ============================================================================
-- VIEW 2: vw_subject_summary
-- Purpose: Complete subject-level summary with key endpoints
-- Use for: Patient counts, disposition, demographics summary
-- ============================================================================
CREATE OR REPLACE VIEW vw_subject_summary AS
SELECT
    -- Identifiers
    adsl.STUDYID,
    adsl.USUBJID,
    adsl.SUBJID,
    adsl.SITEID,

    -- Demographics
    adsl.AGE,
    adsl.AGEGR1,
    adsl.SEX,
    adsl.RACE,
    adsl.ETHNIC,

    -- Treatment
    adsl.TRT01P,
    adsl.TRT01PN,
    adsl.TRT01A,
    adsl.TRT01AN,

    -- Disposition
    adsl.EOSSTT,
    adsl.DCSREAS,
    adsl.DTHFL,

    -- Analysis populations
    adsl.SAFFL,
    adsl.ITTFL,
    adsl.EFFFL,
    adsl.COMP24FL,

    -- Key dates
    adsl.TRTSDT,
    adsl.TRTEDT,
    adsl.TRTDUR,

    -- Computed fields
    CASE
        WHEN adsl.EOSSTT = 'COMPLETED' THEN 'Completer'
        WHEN adsl.EOSSTT = 'DISCONTINUED' THEN 'Discontinuer'
        ELSE 'Ongoing'
    END AS COMPLETION_STATUS,

    CASE
        WHEN adsl.AGE < 18 THEN 'Pediatric'
        WHEN adsl.AGE >= 18 AND adsl.AGE < 65 THEN 'Adult'
        ELSE 'Elderly'
    END AS AGE_CATEGORY,

    -- AE counts (subquery)
    (SELECT COUNT(*) FROM ae WHERE ae.USUBJID = adsl.USUBJID) AS AE_COUNT,
    (SELECT COUNT(*) FROM ae WHERE ae.USUBJID = adsl.USUBJID AND ae.AESER = 'Y') AS SAE_COUNT

FROM adsl;


-- ============================================================================
-- VIEW 3: vw_lab_with_ranges
-- Purpose: Laboratory results with normal ranges and flags
-- Use for: Lab value analysis, shift tables, out-of-range analysis
-- ============================================================================
CREATE OR REPLACE VIEW vw_lab_with_ranges AS
SELECT
    -- Identifiers
    lb.STUDYID,
    lb.USUBJID,
    lb.SUBJID,

    -- Lab test info
    lb.LBSEQ,
    lb.LBTESTCD,
    lb.LBTEST,
    lb.LBCAT,
    lb.LBSCAT,
    lb.LBORRES,
    lb.LBORRESU,
    lb.LBSTRESC,
    lb.LBSTRESN,
    lb.LBSTRESU,

    -- Normal ranges
    lb.LBORNRLO,
    lb.LBORNRHI,
    lb.LBSTNRLO,
    lb.LBSTNRHI,
    lb.LBNRIND,

    -- Visit info
    lb.VISITNUM,
    lb.VISIT,
    lb.LBDTC,
    lb.LBDY,

    -- Treatment (joined)
    adsl.TRT01P AS TREATMENT,

    -- ADaM extensions (if ADLB exists)
    adlb.AVAL,
    adlb.BASE,
    adlb.CHG,
    adlb.PCHG,
    adlb.ABLFL,
    adlb.ANL01FL,

    -- Computed fields
    CASE
        WHEN lb.LBSTRESN < lb.LBSTNRLO THEN 'LOW'
        WHEN lb.LBSTRESN > lb.LBSTNRHI THEN 'HIGH'
        ELSE 'NORMAL'
    END AS RANGE_STATUS,

    CASE
        WHEN lb.LBSTRESN IS NOT NULL AND lb.LBSTNRLO IS NOT NULL AND lb.LBSTNRHI IS NOT NULL THEN
            (lb.LBSTRESN - lb.LBSTNRLO) / NULLIF(lb.LBSTNRHI - lb.LBSTNRLO, 0)
        ELSE NULL
    END AS NORMALIZED_VALUE

FROM lb
LEFT JOIN adsl ON lb.USUBJID = adsl.USUBJID AND lb.STUDYID = adsl.STUDYID
LEFT JOIN adlb ON lb.USUBJID = adlb.USUBJID AND lb.LBSEQ = adlb.LBSEQ;


-- ============================================================================
-- VIEW 4: vw_conmeds
-- Purpose: Concomitant medications with classification
-- Use for: Medication analysis, drug interaction checks
-- ============================================================================
CREATE OR REPLACE VIEW vw_conmeds AS
SELECT
    -- Identifiers
    cm.STUDYID,
    cm.USUBJID,
    cm.SUBJID,

    -- Medication info
    cm.CMSEQ,
    cm.CMTRT,
    cm.CMDECOD,
    cm.CMCAT,
    cm.CMSCAT,
    cm.CMCLAS,
    cm.CMCLASCD,

    -- Dosing
    cm.CMDOSE,
    cm.CMDOSU,
    cm.CMDOSFRQ,
    cm.CMROUTE,

    -- Timing
    cm.CMSTDTC,
    cm.CMENDTC,
    cm.CMSTDY,
    cm.CMENDY,
    cm.CMENRF,

    -- Indication
    cm.CMINDC,

    -- Demographics (joined)
    dm.AGE,
    dm.SEX,
    adsl.TRT01P AS TREATMENT,

    -- Computed fields
    CASE
        WHEN cm.CMENDTC IS NULL OR cm.CMENRF = 'ONGOING' THEN 'Ongoing'
        ELSE 'Completed'
    END AS MEDICATION_STATUS,

    CASE
        WHEN cm.CMSTDY <= 0 THEN 'Prior'
        WHEN cm.CMSTDY > 0 THEN 'On-Treatment'
        ELSE 'Unknown'
    END AS MEDICATION_TIMING

FROM cm
LEFT JOIN dm ON cm.USUBJID = dm.USUBJID AND cm.STUDYID = dm.STUDYID
LEFT JOIN adsl ON cm.USUBJID = adsl.USUBJID AND cm.STUDYID = adsl.STUDYID;


-- ============================================================================
-- VIEW 5: vw_vitals
-- Purpose: Vital signs with baseline and change calculations
-- Use for: Vital sign analysis, safety assessments
-- ============================================================================
CREATE OR REPLACE VIEW vw_vitals AS
SELECT
    -- Identifiers
    vs.STUDYID,
    vs.USUBJID,
    vs.SUBJID,

    -- Vital sign info
    vs.VSSEQ,
    vs.VSTESTCD,
    vs.VSTEST,
    vs.VSCAT,
    vs.VSORRES,
    vs.VSORRESU,
    vs.VSSTRESC,
    vs.VSSTRESN,
    vs.VSSTRESU,

    -- Position
    vs.VSPOS,
    vs.VSLOC,

    -- Visit info
    vs.VISITNUM,
    vs.VISIT,
    vs.VSDTC,
    vs.VSDY,
    vs.VSTPT,
    vs.VSTPTNUM,

    -- Treatment (joined)
    adsl.TRT01P AS TREATMENT,

    -- ADaM extensions (if ADVS exists)
    advs.AVAL,
    advs.BASE,
    advs.CHG,
    advs.PCHG,
    advs.ABLFL,
    advs.ANL01FL,

    -- Computed baseline (fallback if no ADVS)
    FIRST_VALUE(vs.VSSTRESN) OVER (
        PARTITION BY vs.USUBJID, vs.VSTESTCD
        ORDER BY vs.VSDY
    ) AS COMPUTED_BASELINE,

    -- Computed change from baseline
    vs.VSSTRESN - FIRST_VALUE(vs.VSSTRESN) OVER (
        PARTITION BY vs.USUBJID, vs.VSTESTCD
        ORDER BY vs.VSDY
    ) AS COMPUTED_CHG

FROM vs
LEFT JOIN adsl ON vs.USUBJID = adsl.USUBJID AND vs.STUDYID = adsl.STUDYID
LEFT JOIN advs ON vs.USUBJID = advs.USUBJID AND vs.VSSEQ = advs.VSSEQ;


-- ============================================================================
-- VIEW 6: vw_exposure
-- Purpose: Study drug exposure summary
-- Use for: Exposure analysis, compliance calculations
-- ============================================================================
CREATE OR REPLACE VIEW vw_exposure AS
SELECT
    -- Identifiers
    ex.STUDYID,
    ex.USUBJID,
    ex.SUBJID,

    -- Treatment info
    ex.EXTRT,
    ex.EXDOSE,
    ex.EXDOSU,
    ex.EXDOSFRQ,
    ex.EXROUTE,

    -- Timing
    ex.EXSTDTC,
    ex.EXENDTC,
    ex.EXSTDY,
    ex.EXENDY,

    -- Duration
    ex.EXDUR,

    -- Treatment (from ADSL)
    adsl.TRT01P AS PLANNED_TREATMENT,
    adsl.TRT01A AS ACTUAL_TREATMENT,
    adsl.TRTDUR AS TOTAL_TREATMENT_DURATION,

    -- Demographics
    dm.AGE,
    dm.SEX,

    -- Computed fields
    CASE
        WHEN ex.EXENDY IS NOT NULL AND ex.EXSTDY IS NOT NULL THEN
            ex.EXENDY - ex.EXSTDY + 1
        ELSE NULL
    END AS EXPOSURE_DAYS

FROM ex
LEFT JOIN dm ON ex.USUBJID = dm.USUBJID AND ex.STUDYID = dm.STUDYID
LEFT JOIN adsl ON ex.USUBJID = adsl.USUBJID AND ex.STUDYID = adsl.STUDYID;


-- ============================================================================
-- VIEW 7: vw_efficacy_endpoints
-- Purpose: Key efficacy endpoints with treatment comparison
-- Use for: Primary/secondary endpoint analysis
-- ============================================================================
CREATE OR REPLACE VIEW vw_efficacy_endpoints AS
SELECT
    -- Identifiers
    adsl.STUDYID,
    adsl.USUBJID,
    adsl.SUBJID,
    adsl.SITEID,

    -- Treatment
    adsl.TRT01P,
    adsl.TRT01PN,

    -- Demographics
    adsl.AGE,
    adsl.SEX,
    adsl.RACE,

    -- Population flags
    adsl.SAFFL,
    adsl.ITTFL,
    adsl.EFFFL,

    -- Efficacy endpoints (example - adjust based on actual study)
    adeff.PARAMCD,
    adeff.PARAM,
    adeff.AVAL,
    adeff.BASE,
    adeff.CHG,
    adeff.PCHG,
    adeff.AVISIT,
    adeff.AVISITN,
    adeff.ABLFL,
    adeff.ANL01FL,

    -- Response categories
    CASE
        WHEN adeff.CHG IS NOT NULL AND adeff.CHG < 0 THEN 'Improved'
        WHEN adeff.CHG IS NOT NULL AND adeff.CHG > 0 THEN 'Worsened'
        WHEN adeff.CHG IS NOT NULL AND adeff.CHG = 0 THEN 'No Change'
        ELSE 'Unknown'
    END AS RESPONSE_CATEGORY

FROM adsl
LEFT JOIN adeff ON adsl.USUBJID = adeff.USUBJID AND adsl.STUDYID = adeff.STUDYID
WHERE adsl.EFFFL = 'Y';


-- ============================================================================
-- UTILITY VIEW: vw_study_overview
-- Purpose: High-level study statistics
-- Use for: Dashboard, study overview queries
-- ============================================================================
CREATE OR REPLACE VIEW vw_study_overview AS
SELECT
    adsl.STUDYID,

    -- Subject counts
    COUNT(DISTINCT adsl.USUBJID) AS TOTAL_SUBJECTS,
    COUNT(DISTINCT CASE WHEN adsl.SAFFL = 'Y' THEN adsl.USUBJID END) AS SAFETY_POP,
    COUNT(DISTINCT CASE WHEN adsl.ITTFL = 'Y' THEN adsl.USUBJID END) AS ITT_POP,
    COUNT(DISTINCT CASE WHEN adsl.EFFFL = 'Y' THEN adsl.USUBJID END) AS EFFICACY_POP,

    -- Completion status
    COUNT(DISTINCT CASE WHEN adsl.EOSSTT = 'COMPLETED' THEN adsl.USUBJID END) AS COMPLETERS,
    COUNT(DISTINCT CASE WHEN adsl.EOSSTT = 'DISCONTINUED' THEN adsl.USUBJID END) AS DISCONTINUERS,

    -- Demographics summary
    AVG(adsl.AGE) AS MEAN_AGE,
    MIN(adsl.AGE) AS MIN_AGE,
    MAX(adsl.AGE) AS MAX_AGE,

    -- Treatment counts
    COUNT(DISTINCT adsl.TRT01P) AS N_TREATMENTS,

    -- Site counts
    COUNT(DISTINCT adsl.SITEID) AS N_SITES

FROM adsl
GROUP BY adsl.STUDYID;


-- ============================================================================
-- INDEXES for performance (create if using persistent views)
-- ============================================================================
-- Note: DuckDB handles indexing automatically for views
-- For materialized views, consider adding indexes on:
-- - USUBJID (all views)
-- - STUDYID (all views)
-- - TRT01P (subject_summary, efficacy_endpoints)
-- - VISITNUM (lab_with_ranges, vitals)
-- - AEDECOD (ae_with_demographics)


-- ============================================================================
-- GRANT permissions (for multi-user environments)
-- ============================================================================
-- GRANT SELECT ON ALL VIEWS TO readonly_user;
-- GRANT SELECT ON ALL VIEWS TO analyst_role;
