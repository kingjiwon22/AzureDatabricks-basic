# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Scenario 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### UI를 통해 File Upload 하여 테이블 생성하기
# MAGIC UI를 사용하여 로컬 시스템에서 작은 CSV 또는 TSV 파일을 가져와 Table을 생성할 수 있습니다.  
# MAGIC 이 때 한번에 최대 10개의 파일 업로드를 지원하고, 총 크기가 100MB 미만이어야 가능합니다.  
# MAGIC 방법은 총 3가지가 있습니다.
# MAGIC - New > File Upload
# MAGIC - New > Data > Upload data
# MAGIC - Data > Add > Add Data > Upload data

# COMMAND ----------

# MAGIC %md
# MAGIC **`New > File Upload`** 를 클릭합니다.  
# MAGIC 파일 브라우저 버튼을 클릭하거나 드롭 영역에 직접 파일을 끌어다 놓습니다.  
# MAGIC 테이블을 미리 보고 구성할 수 있습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC 생성된 테이블을 **`Data > Data Explorer`** 에서 확인해보겠습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC File Upload로 생성한 테이블을 확인해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 인사정보 테이블
# MAGIC select *
# MAGIC  from Emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 부서정보 테이블
# MAGIC select *
# MAGIC  from Dept;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python & Databricks SQL 활용하여 데이터 생성 및 활용하기  
# MAGIC 지금부터는 Magic Command 활용하여 SQL로 테이블을 생성하고, Python으로 무작위 데이터를 생성하여 테이블에 적재해보겠습니다.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Table 생성  
# MAGIC SQL로 테이블을 생성하기 위해 셀의 우측에서 사용언어를 SQL로 선택합니다.  
# MAGIC 이제 Create Table문으로 Project 테이블과 EmpDetail 테이블을 생성하겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- 테이블 생성 
# MAGIC create table if not exists Project(
# MAGIC 	ProjCode string,
# MAGIC 	ProjName string,
# MAGIC 	CreatedWhen string,
# MAGIC 	LastEditedWhen string,
# MAGIC 	LastEditedBy string
# MAGIC );
# MAGIC 
# MAGIC create table if not exists EmpDetail(
# MAGIC 	`사원번호` string,
# MAGIC 	`사원명` string,
# MAGIC 	`직무` string,
# MAGIC 	`입사일` string,
# MAGIC 	`입사일수` string,
# MAGIC     `부서코드` string, 
# MAGIC     `부서명` string,
# MAGIC     `부서위치` string,
# MAGIC     `진행프로젝트수` int,
# MAGIC     `누적상여금` int,
# MAGIC     `최종상여금지급일` string,
# MAGIC     `스케쥴최종수행일` string
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 생성한 테이블에 무작위 데이터 적재  
# MAGIC Project 테이블에 넣어줄 데이터를 파이썬을 사용하여 만들어보겠습니다.  
# MAGIC 데이터를 넣어주기 전, Project 테이블을 조회하여 데이터가 없음을 확인해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) cnt
# MAGIC  from Project;

# COMMAND ----------

# MAGIC %md
# MAGIC 이제 Python의 Random 모듈 내 Sample 함수를 통해 무작위 데이터를 10건 생성하고,  
# MAGIC 실행할 쿼리문을 *query_all* 이라는 변수에 담아서 출력한 후, 이를 spark.sql을 통해 실행하겠습니다.  

# COMMAND ----------

#Project에 데이터 insert 
import random
from datetime import datetime

smpl1 = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z','0','1','2','3','4','5','6','7','8','9']
smpl2 = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
smpl3 = ['0','1','2','3','4','5','6','7','8','9']

i =1

today = datetime.today().strftime('%Y-%m-%d')

while i <= 10  :
    smplCode1 = random.sample(smpl1,7)
    projCode = ''.join(smplCode1)
    
    smplCode2 = random.sample(smpl2,5)
    projName = ''.join(smplCode2)
    
    smplCode3 = random.sample(smpl3,1)
    admin = "admin"+''.join(smplCode3)

    query = "'"+projCode + "','"+projName+"','"+today+"','"+today+"','"+admin+"'"
    #print(query)
    query_all = "insert into Project values("+ query+")"
    print(query_all)
    spark.sql(query_all)
    i = i + 1

# COMMAND ----------

# MAGIC %md
# MAGIC 데이터 적재가 완료되었다면, Project 테이블을 조회해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC  from Project;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Project, Emp, Dept 데이터를 통해 EmpDetail에 데이터 적재(Mart Table)  
# MAGIC 위의 과정들을 통해서 Project, Emp, Dept 총 3개의 소스 테이블이 준비되었습니다.  
# MAGIC 이 3개의 테이블에서 사원별로 진행중인 프로젝트 수를 집계하여 EmpDetail에 적재해보겠습니다.  

# COMMAND ----------

# MAGIC %md
# MAGIC 우선  EmpDetail 테이블을 조회하여 데이터가 없음을 확인해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) cnt
# MAGIC  from EmpDetail;

# COMMAND ----------

# MAGIC %md
# MAGIC Project, Emp, Dept 테이블을 Join하여 EmpDetail에 넣어줄 데이터를 조회해보겠습니다.

# COMMAND ----------

# MAGIC %sql
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
# MAGIC order by e.EmpId;

# COMMAND ----------

# MAGIC %md
# MAGIC Insert - Select 문을 통해 EmpDetail에 데이터를 적재하겠습니다.  
# MAGIC 이 작업은 Schedule 기능을 사용하여 해보겠습니다.
