### 10. **Critical Bug Fixes for Silver/Gold Jobs** ✅ FIXED (Jan 2026)
|- **Issues Fixed:**
  1. **MERGE INTO Syntax Error**
     - **Problem:** Iceberg doesn't support `UPDATE SET *` or `INSERT *`
     - **Impact:** All batch jobs failed immediately during MERGE operations
     - **Solution:** Rewrote with explicit column lists for UPDATE and INSERT
     - **Files Changed:**
       - `spark/jobs/silver_batch_job.py` (line 266-312)
       - `spark/jobs/gold_batch_job.py` (lines 354-421)
  
  2. **Missing schema_version Column**
     - **Problem:** Bronze has schema_version, Silver/Gold don't → schema mismatch on MERGE
     - **Impact:** Column count mismatch causing MERGE failures
     - **Solution:** Added schema_version column to Silver/Gold tables and transformations
     - **Files Changed:**
       - `spark/jobs/silver_batch_job.py` (line 26, line 168, line 313)
       - `spark/jobs/gold_batch_job.py` (line 29, lines 121, 253)
       - Added SCHEMA_VERSION = "1.0.0" constant in both jobs
  
  3. **Iceberg Format Version Inconsistency**
     - **Problem:** Documentation claimed v3 but code used v2 (EMR 7.6.0 limitation)
     - **Impact:** Not using v3 features (deletion vectors, row lineage)
     - **Solution:** Upgraded to EMR 7.12.0 with Iceberg 1.10.0 (v3 support)
     - **Files Changed:**
       - `infrastructure/terraform/main.tf` (line 721: emr-7.12.0)
       - All jobs: Updated `format-version='3'` and `merge-on-read` modes
       - Bronze: `spark/jobs/bronze_streaming_job.py` (line 361)
       - Silver: `spark/jobs/silver_batch_job.py` (line 237)
       - Gold: `spark/jobs/gold_batch_job.py` (lines 309, 338)
       - Iceberg runtime: 1.6.1 → 1.10.0 (in Terraform)
  
  4. **Missing Spark Optimizations**
     - **Problem:** Silver/Gold/DataQuality jobs missing adaptive query execution configs
     - **Impact:** Poor performance on skewed data
     - **Solution:** Added Spark optimization configs
     - **Files Changed:**
       - `spark/jobs/silver_batch_job.py` (line 70)
       - `spark/jobs/gold_batch_job.py` (line 54)
       - `spark/jobs/data_quality_job.py` (lines 42-47)
       - Terraform Step Functions: Added optimization params to spark-submit

|- **Status:** All critical bugs fixed, jobs should now execute successfully

