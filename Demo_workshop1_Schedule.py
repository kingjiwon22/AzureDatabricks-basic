# Databricks notebook source
# MAGIC %md
# MAGIC ###Schedule (작업예약)

# COMMAND ----------

# MAGIC %md
# MAGIC 먼저 Schedule로 수행할 작업들을 살펴보겠습니다.

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Schedule 수행 전 EmpDetail 조회하기
# MAGIC select *
# MAGIC  from EmpDetail;

# COMMAND ----------

# MAGIC %sql
# MAGIC --기존 데이터 삭제하기
# MAGIC delete from EmpDetail;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Mart: Project Count Group by Emp
# MAGIC -- EmpDetail에 데이터 적재
# MAGIC insert into EmpDetail
# MAGIC select  e.EmpId `사원번호`
# MAGIC       , max(e.EmpName) `사원명`
# MAGIC       , max(e.Job) `직무`
# MAGIC       , max(e.HireDate) `입사일`
# MAGIC       , max(datediff(current_date, e.HireDate)) `입사일수` 
# MAGIC       , max(d.DeptId) `부서코드`
# MAGIC       , max(d.DeptName) `부서명`
# MAGIC       , max(d.Location) `부서위치`
# MAGIC       , count(p.ProjCode) `진행 프로젝트 수` 
# MAGIC       , max(e.comm) `누적상여금`
# MAGIC       , '9999-12-31' `최종상여금지급일`
# MAGIC       , current_date `스케쥴최종수행일`
# MAGIC from    emp e 
# MAGIC inner join dept d on e.DeptId = d.DeptId
# MAGIC inner join project p on e.empid = p.LastEditedBy
# MAGIC group by e.EmpId
# MAGIC order by e.EmpId

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- EmpDetail 조회하여 데이터가 잘 적재되었는지 확인하기
# MAGIC select *
# MAGIC  from EmpDetail;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schedule 등록하기  
# MAGIC 우측 상단의 **`Schedule`** 버튼을 클릭합니다.  
# MAGIC 작업명, 스케쥴 주기 설정, 작업할 Cluster, 변수 설정, 그리고 시작/결과 알람을 E-mail로 받아볼 수 있는 설정이 가능합니다.  
# MAGIC **`Create`** 버튼을 클릭하면 해당 노트북에 작업이 예약됩니다. 

# COMMAND ----------

# MAGIC %md
# MAGIC #### 등록한 Schedule 확인하기  
# MAGIC 등록한 Schedule은 **`Workflows > Jobs`** 에서 확인할 수 있습니다.(View in Workflows)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 실행된 Schedule의 알림 메일 확인하기(PPT 참고)
# MAGIC 실행된 Schedule의 시작, 작업 결과를 E-mail로 확인할 수 있습니다.
