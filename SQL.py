# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC -- 부서별 진행수 집계
# MAGIC 
# MAGIC select `부서코드`, `부서명`, sum(`진행프로젝트수`) as `진행프로젝트수`
# MAGIC  from empdetail
# MAGIC  group by `부서코드`,`부서명`
# MAGIC  order by `부서코드`,`부서명`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 직무별 상여금 집계
# MAGIC select Job, sum(Comm) as Tot_Comm
# MAGIC  from emp
# MAGIC group by Job
# MAGIC order by Tot_Comm desc

# COMMAND ----------


