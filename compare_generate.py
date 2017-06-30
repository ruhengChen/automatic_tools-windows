#-*- coding=utf-8 -*-
import pyodbc
import re
import sys
import logging
import os
# import ftp
import subprocess
import time

local_time = time.strftime("%Y-%m-%d", time.localtime())


class Logger:        
    def __init__(self, logName, logFile):
        self._logger = logging.getLogger(logName)
        handler = logging.FileHandler(logFile,mode='w')
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def log(self, msg):
        if self._logger is not None:
            self._logger.info(msg)

## 新增表处理
def deal_table_add(tablelist,date):
    read_me_log.log("增加下发表:\n"+', '.join(tablelist)+'\n')
    for tablename in tablelist:
        deal_table_all(tablename,date,tabspace)
        syscode, tablenm = tablename.split('.')
        tablestr = tablename.replace('.','_')
        job.log("--add table")
        job.log("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('LD_ODS_%s_INIT','DAY','CMD','L_ODSLD','load_from_files_to_nds.sh','%s %s $dateid ALL','5','1','LD_ODS_%s_INIT','Y',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr,tablenm,syscode,tablestr,syscode,ip))
        job.log("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s_INIT','DAY','SQL','L_ODS','AP_ODS_%s_INIT.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s_INIT','Y',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr,tablestr,tablestr,syscode,ip))
        job.log("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('LD_ODS_%s','DAY','CMD','L_ODSLD','load_from_files_to_nds.sh','%s %s $dateid ADD','5','1','LD_ODS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr,tablenm,syscode,tablestr,syscode,ip))
        job.log("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s','DAY','SQL','L_ODS','AP_ODS_%s.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(tablestr,tablestr,tablestr,syscode,ip))
        job.log("INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('AP_ODS_%s_YATOPUPDATE','DAY','SQL','L_ODS','AP_ODS_%s_YATOPUPDATE.SQL','EDW /etl/etldata/script/odssql','5','1','AP_ODS_%s_YATOPUPDATE','U',CURRENT TIMESTAMP,'1','1','%s','','%s');\n" %(tablestr,tablestr,tablestr,syscode,ip))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('LD_ODS_%s_INIT','UNCOMPRESS_INIT',CURRENT TIMESTAMP);" %tablestr)
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('LD_ODS_%s','UNCOMPRESS_%s',CURRENT TIMESTAMP);" %(tablestr,syscode))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','LD_ODS_%s',CURRENT TIMESTAMP);" %(tablestr,tablestr))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','LD_ODS_%s_INIT',CURRENT TIMESTAMP);\n" %(tablestr,tablestr))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_INIT','LD_ODS_%s_INIT',CURRENT TIMESTAMP);" %(tablestr,tablestr))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('ODS_DONE','AP_ODS_%s',CURRENT TIMESTAMP);" %tablestr)
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('ODS_DONE_INIT','AP_ODS_%s_INIT',CURRENT TIMESTAMP);" %tablestr)
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','LD_ODS_%s',CURRENT TIMESTAMP);" %(tablestr,tablestr))
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s_YATOPUPDATE','UNCOMPRESS_INIT',CURRENT TIMESTAMP);" %tablestr)
        job.log("INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('AP_ODS_%s','AP_ODS_%s_YATOPUPDATE',CURRENT TIMESTAMP);" %(tablestr,tablestr))
        
        job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
        job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
        job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='LD_ODS_%s_INIT';" %tablestr)
        job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='W' WHERE JOB_NM ='LD_ODS_%s';\n" %tablestr)

## 删除表处理
def deal_table_del(tablelist,date):
    read_me_log.log("删除下发表:\n"+', '.join(tablelist)+'\n')
    for tablename in tablelist:
        print("del table " + tablename)
        tablestr = tablename.replace('.','_')
        job.log("--del table")
        job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG ='X' WHERE JOB_NM LIKE '%_ODS_{}%';".format(tablestr))

## 比较两次变更的表的不同之处
def get_differ_table(olddate,newdate):

    old_tablelist = []
    sql = "select distinct src_stm_id||'.'||tab_code from DSA.ORGIN_TABLE_DETAIL where change_date={0}".format(olddate)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    for row in rows:
        old_tablelist.append(row[0])

    old_set = set(old_tablelist)


    new_tablelist = []
    sql = "select distinct src_stm_id||'.'||tab_code from DSA.ORGIN_TABLE_DETAIL where change_date={0}".format(newdate)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    for row in rows:
        new_tablelist.append(row[0])

    new_set = set(new_tablelist)

    add_set = new_set - old_set
    del_set = old_set - new_set
    common_set = old_set & new_set
    
    if add_set:
        # print("add_tables:" , add_set)
        deal_table_add(list(add_set),newdate)
    
    if del_set:
        deal_table_del(list(del_set),newdate)

    # deal_list = list(add_set | common_set)
    deal_list = list(common_set)
    return deal_list

def get_column_detail(tablename,date):
    syscode, tablenm = tablename.split('.')
    sql = "select field_code,data_tp||','||length||','||precsn,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}'".format(tablenm,date,syscode)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    old_column_list = []
    old_column_dict = {}
    # print(sql)
    for row in rows:
        old_column_list.append(row[0])
        old_column_dict[row[0]] = row[1]+','+row[2]
        # print(old_column_set+set(row))

    old_column_set = set(old_column_list)
    # print(old_column_list)

    # print(old_column_set)
    return old_column_dict,old_column_set

## 生成apsql_init
def generate_ap_sql_init(field_code_list,delta_tablename,tablename,date):
    print(tablename + ":generate ap_sql_init ")
    file = open(date+'/AP/AP_ODS_'+tablename.replace('.','_')+'_INIT.SQL','w')
    file.write('SELECT \'Rows readed:\',COUNT(1),\'Rows changed:\',COUNT(1) FROM (SELECT 1 FROM '+delta_tablename+') S;\n')
    file.write('VALUES(\'Rows updated:\',0);\n')

    src_list = [ 'S.'+field_code for field_code in field_code_list ]
    src_str = ','.join(src_list) + ",'#DATEOFDATA#','9999-12-31',New_JOB_SEQ_ID FROM "
    file.write("DECLARE MYCUR CURSOR FOR SELECT "+src_str+delta_tablename+" S;\n")

    des_str = ','.join(field_code_list)
    file.write("LOAD FROM MYCUR OF CURSOR REPLACE INTO "+tablename+'('+des_str+',EFF_DT,END_DT,JOB_SEQ_ID);\n')
    file.close()

    # ftp_ap_sql('AP_'+tablename.replace('.','_')+'_INIT.SQL', date)

    print(tablename + ":generate ap_sql_init success")

## 生成apsql
def generate_ap_sql(field_code_list,delta_tablename,tablename,date,his_tablename,primary_list):
    print(tablename+":generate ap_sql")
    field_code_str = ','.join(field_code_list)+',EFF_DT,END_DT,JOB_SEQ_ID'
    file = open(date+'/AP/AP_ODS_'+tablename.replace('.','_')+'.SQL','w')
    file.write("SELECT \'Rows updated:\',COUNT(1) FROM (SELECT 1 FROM "+delta_tablename+" WHERE ETL_FLAG IN (\'A\',\'D\')) S;\n\n")
    file.write('--REDO：DELETE LAST JOB LOADED DATA\n')
    file.write('DELETE FROM '+tablename+' WHERE JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
    file.write('--REDO：RECOVERY DATA FROM HISTORY TABLE\n')
    file.write('INSERT INTO '+tablename+'('+field_code_str+')\n')
    file.write('select '+field_code_str+'\nfrom '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
    file.write('--REDO：DELETE HISTORY DATA\n')
    file.write('DELETE FROM '+his_tablename+' WHERE NEW_JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
    file.write('--BACKUP DATA TO HISTROY TABLE\n')
    file.write('SELECT \'Rows readed:\',COUNT(1),\'Rows changed:\',COUNT(1) FROM (SELECT 1 FROM '+delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'A\',\'D\')) S;\n')
    file.write('SELECT \'Rows updated:\',COUNT(1) FROM NEW TABLE (\n')
    file.write('INSERT INTO '+his_tablename+'('+field_code_str+',NEW_JOB_SEQ_ID)\n')
    file.write('select '+field_code_str+',New_JOB_SEQ_ID\n from '+ tablename+' T\n')
    file.write('WHERE T.END_DT=\'9999-12-31\' AND EXISTS ( SELECT 1 FROM '+delta_tablename+' S\n')
    if primary_list:
        primary_str = ' AND '.join("T.%s=S.%s" %(x,x) for x in primary_list)
    else:
        primary_str=' AND '.join("T.%s=S.%s" %(x,x) for x in field_code_list)
    file.write('WHERE '+primary_str+' ));\n\n')
    file.write('--DROP ZIPPER\n')
    file.write('MERGE INTO '+tablename+' T \nUSING (SELECT * FROM '
        +delta_tablename+' WHERE ETL_FLAG IN (\'I\',\'D\',\'A\')) S\nON '
        +primary_str+'  AND T.END_DT=\'9999-12-31\' \n   WHEN MATCHED THEN UPDATE SET \nT.END_DT=\'#DATEOFDATA#\', T.JOB_SEQ_ID= New_JOB_SEQ_ID;\n\n')
    file.write('--CREATE ZIPPER\nINSERT INTO '+tablename+'('+field_code_str+')\nselect '+','.join(field_code_list)+',\'#DATEOFDATA#\',\'9999-12-31\',New_JOB_SEQ_ID\n')
    file.write('from '+delta_tablename+' where ETL_FLAG in (\'A\',\'I\');\n\n')
    file.write('--CONFERM DATA INTEGRITY\n')
    file.write('MERGE INTO '+tablename+' T \nUSING (SELECT * FROM '+delta_tablename+' WHERE ETL_FLAG = \'D\' ) S\n')
    file.write('ON '+primary_str+'\nWHEN NOT MATCHED THEN\nINSERT ('+field_code_str+')\n')
    file.write('VALUES ('+','.join(field_code_list)+',\'#DATEOFDATA#\',\'#DATEOFDATA#\',New_JOB_SEQ_ID);')
    file.close()

    # ftp_ap_sql('AP_'+tablename.replace('.','_')+'.SQL', date)
    print(tablename+":generate ap_sql success")

## 生成delta表建表语句
def generate_delta_ddl(tablename,date,tabspace,tabletype):
    print(tablename+":generate delta ddl ...")
    syscode, tablenm = tablename.split('.')
    sql = "select field_code,data_tp,length,precsn,case when primary_key_flag='' then 'N' else 'Y' end, tab_nm,field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,date,syscode)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()

    delta_tablename = "DELTA."+tablename.replace('.','_')
    
    delta_log.log("------------------------------------------------")
    delta_log.log("-- DDL Statements for Table " + delta_tablename)
    delta_log.log("------------------------------------------------")

    if tabletype == "add column":
        delta_log.log("DROP TABLE "+delta_tablename+";")
    delta_log.log("CREATE TABLE "+delta_tablename+' (')

    num = 0 
    primary_list = []
    field_code_dict = {}
    field_code_list = []
    for row in rows:
        # print(row)
        field_code, filed_type, length, precsn, is_primary, table_comment, field_comment = row
        field_code_list.append(field_code)
        # print(field_code, filed_type, is_primary, table_comment, field_comment)
        field_code_dict[field_code] = field_comment
        if filed_type == "CHARACTER":
            filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
        elif filed_type == "DECIMAL":
            filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
        elif filed_type == "VARCHAR":
            filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
        else:
            filed_line = field_code +'\t'+ filed_type

        if is_primary == "Y":
            filed_line = filed_line + "\t NOT NULL"
            primary_list.append(field_code)

        if num != 0:
            filed_line = ',' + filed_line

        num += 1
        delta_log.log(filed_line)

    delta_log.log(",ETL_DT\tDATE")
    delta_log.log(",ETL_FLAG\tCHARACTER(1)\tWith Default 'I'")
    delta_log.log(') ' + ' IN ' + tabspace)

    primary_str = ','.join(primary_list)

    if primary_str:
        delta_log.log("Partitioning Key ("+primary_str+") Using Hashing")
        
    delta_log.log("Compress Yes;")

    if table_comment:
        delta_log.log("Comment on Table "+delta_tablename+" is "+'\''+table_comment+'\';')

    for field_code,field_comment in field_code_dict.items():
        delta_log.log("Comment on Column "+delta_tablename+'.'+field_code+'\tis \''+field_comment+'\';')

    if primary_str:
        delta_log.log('')
        delta_log.log('--------------------------------------------------')
        delta_log.log('-- Create Index '+delta_tablename)
        delta_log.log('--------------------------------------------------')
        delta_log.log('create  Index '+delta_tablename)
        delta_log.log('   on '+delta_tablename)
        delta_log.log('   ('+primary_str+')    Allow Reverse Scans;\n')
    
    print(tablename+":generate delta success")

## 生产ods建表语句
def generate_ods_ddl(tablename,date,tabspace,tabletype):
    print(tablename+":generate ods ddl ...")
    syscode, tablenm = tablename.split('.')
    sql = "select field_code,data_tp,length,precsn,case when primary_key_flag='' then 'N' else 'Y' end, tab_nm,field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,date,syscode)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    
    all_log.log("------------------------------------------------")
    all_log.log("-- DDL Statements for Table " + tablename)
    all_log.log("------------------------------------------------")

    if tabletype == "add column":
        all_log.log("DROP TABLE "+tablename+";")

    all_log.log("CREATE TABLE "+tablename+' (')

    num = 0 
    primary_list = []
    field_code_dict = {}
    field_code_list = []
    for row in rows:
        # print(row)
        field_code, filed_type, length, precsn, is_primary, table_comment, field_comment = row
        field_code_list.append(field_code)
        # print(field_code, filed_type, is_primary, table_comment, field_comment)
        field_code_dict[field_code] = field_comment
        if filed_type == "CHARACTER":
            filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
        elif filed_type == "DECIMAL":
            filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
        elif filed_type == "VARCHAR":
            filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
        else:
            filed_line = field_code +'\t'+ filed_type

        if is_primary == "Y":
            filed_line = filed_line + "\t NOT NULL"
            primary_list.append(field_code)

        if num != 0:
            filed_line = ',' + filed_line

        # filed_line = filed_line + '\tCOMMENT\t\''+field_comment+'\''
        num += 1
        all_log.log(filed_line)
    
    all_log.log(",EFF_DT\tDATE\tNOT NULL")
    all_log.log(",END_DT\tDATE")
    all_log.log(",JOB_SEQ_ID\tINTEGER")
    all_log.log(') ' + ' IN ' + tabspace)

    tmp_primary_list = primary_list[:]
    tmp_primary_list.append('EFF_DT')

    tmp_primary_str = ','.join(tmp_primary_list)

    primary_str = ','.join(primary_list)

    if tmp_primary_str:
        all_log.log("Partitioning Key ("+tmp_primary_str+") Using Hashing")

    all_log.log("Compress Yes;")

    if table_comment:
        all_log.log("Comment on Table "+tablename+" is "+'\''+table_comment+'\';')

    for field_code,field_comment in field_code_dict.items():
        all_log.log("Comment on Column "+tablename+'.'+field_code+'\tis \''+field_comment+'\';')
        # print(field_code)

    all_log.log('')
    all_log.log('--------------------------------------------------')
    all_log.log('-- Create Index '+tablename+'_'+local_time+'_1')
    all_log.log('--------------------------------------------------')
    all_log.log('create  Index '+tablename+'_'+local_time+'_1')
    all_log.log('   on '+tablename)
    all_log.log('   (END_DT)    Allow Reverse Scans;\n')

    all_log.log('--------------------------------------------------')
    all_log.log('-- Create Index '+tablename+'_'+local_time+'_2')
    all_log.log('--------------------------------------------------')
    all_log.log('create  Index '+tablename+'_'+local_time+'_2')
    all_log.log('   on '+tablename)
    all_log.log('   (JOB_SEQ_ID)    Allow Reverse Scans;\n')

    if primary_str:
        all_log.log('')
        all_log.log("-- DDL Statements for Primary Key on Table ")
        all_log.log("ALTER TABLE "+tablename+" ADD PRIMARY KEY ("+tmp_primary_str+");")
        all_log.log('')
    
    print(tablename+":generate ods success")

## 生产his建表语句
def generate_his_ddl(tablename,date,tabspace,tabletype):
    print(tablename+":generate his ddl ...")
    syscode, tablenm = tablename.split('.')
    sql = "select field_code,data_tp,length,precsn,case when primary_key_flag='' then 'N' else 'Y' end, tab_nm,field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,date,syscode)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()

    his_tablename = "ODSHIS."+tablename.replace('.','_')
    his_log.log("------------------------------------------------")
    his_log.log("-- DDL Statements for Table " + his_tablename)
    his_log.log("------------------------------------------------")

    if tabletype == "add column":
        his_log.log("DROP TABLE "+his_tablename+";")

    his_log.log("CREATE TABLE "+his_tablename+' (')

    num = 0 
    primary_list = []
    field_code_dict = {}
    field_code_list = []
    table_comment = ""
    for row in rows:
        # print(row)
        field_code, filed_type, length, precsn, is_primary, table_comment, field_comment = row
        field_code_list.append(field_code)
        # print(field_code, filed_type, is_primary, table_comment, field_comment)
        field_code_dict[field_code] = field_comment
        if filed_type == "CHARACTER":
            filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
        elif filed_type == "DECIMAL":
            filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
        elif filed_type == "VARCHAR":
            filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
        else:
            filed_line = field_code +'\t'+ filed_type

        if is_primary == "Y":
            filed_line = filed_line + "\t NOT NULL"
            primary_list.append(field_code)

        if num != 0:
            filed_line = ',' + filed_line

        # filed_line = filed_line + '\tCOMMENT\t\''+field_comment+'\''
        num += 1
        his_log.log(filed_line)

    his_log.log(",EFF_DT\tDATE\tNOT NULL")
    his_log.log(",END_DT\tDATE")
    his_log.log(",JOB_SEQ_ID\tINTEGER")
    his_log.log(",NEW_JOB_SEQ_ID\tINTEGER")
    his_log.log(') ' + ' IN ' + tabspace)

    primary_str = ','.join(primary_list)

    if primary_str:
        his_log.log("Partitioning Key ("+primary_str+") Using Hashing")
        
    delta_log.log("Compress Yes;")
    all_log.log("Compress Yes;")
    his_log.log("Compress Yes;")

    if table_comment:
        his_log.log("Comment on Table "+his_tablename+" is "+'\''+table_comment+'\';')

    for field_code,field_comment in field_code_dict.items():
        his_log.log("Comment on Column "+his_tablename+'.'+field_code+'\tis \''+field_comment+'\';')

    his_log.log('')
    his_log.log('--------------------------------------------------')
    his_log.log('-- Create Index '+his_tablename)
    his_log.log('--------------------------------------------------')
    his_log.log('create  Index '+his_tablename)
    his_log.log('   on '+his_tablename)
    his_log.log('   (NEW_JOB_SEQ_ID)    Allow Reverse Scans;\n')

    print(tablename+":generate his success")

## 同时生成ods,his和dealta表
def deal_table_all(tablename,date,tabspace):
    print(tablename+":generate ddl ...")
    syscode, tablenm = tablename.split('.')
    sql = "select field_code,data_tp,length,precsn,case when primary_key_flag='' then 'N' else 'Y' end, tab_nm,field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,date,syscode)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()

    delta_tablename = "DELTA."+tablename.replace('.','_')
    his_tablename = "ODSHIS."+tablename.replace('.','_')
    
    delta_log.log("------------------------------------------------")
    all_log.log("------------------------------------------------")
    his_log.log("------------------------------------------------")

    delta_log.log("-- DDL Statements for Table " + delta_tablename)
    all_log.log("-- DDL Statements for Table " + tablename)
    his_log.log("-- DDL Statements for Table " + his_tablename)

    delta_log.log("------------------------------------------------")
    all_log.log("------------------------------------------------")
    his_log.log("------------------------------------------------")

    delta_log.log("CREATE TABLE "+delta_tablename+' (')
    all_log.log("CREATE TABLE "+tablename+' (')
    his_log.log("CREATE TABLE "+his_tablename+' (')


    num = 0 
    primary_list = []
    field_code_dict = {}
    field_code_list = []
    table_comment = ""
    for row in rows:
        # print(row)
        field_code, filed_type, length, precsn, is_primary, table_comment, field_comment = row
        field_code_list.append(field_code)
        # print(field_code, filed_type, is_primary, table_comment, field_comment)
        field_code_dict[field_code] = field_comment
        if filed_type == "CHARACTER":
            filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
        elif filed_type == "DECIMAL":
            filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
        elif filed_type == "VARCHAR":
            filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
        else:
            filed_line = field_code +'\t'+ filed_type

        if is_primary == "Y":
            filed_line = filed_line + "\t NOT NULL"
            primary_list.append(field_code)

        if num != 0:
            filed_line = ',' + filed_line

        # filed_line = filed_line + '\tCOMMENT\t\''+field_comment+'\''
        num += 1
        delta_log.log(filed_line)
        all_log.log(filed_line)
        his_log.log(filed_line)
    
    all_log.log(",EFF_DT\tDATE\tNOT NULL")
    all_log.log(",END_DT\tDATE")
    all_log.log(",JOB_SEQ_ID\tINTEGER")

    delta_log.log(",ETL_DT\tDATE")
    delta_log.log(",ETL_FLAG\tCHARACTER(1)\tWith Default 'I'")

    his_log.log(",EFF_DT\tDATE\tNOT NULL")
    his_log.log(",END_DT\tDATE")
    his_log.log(",JOB_SEQ_ID\tINTEGER")
    his_log.log(",NEW_JOB_SEQ_ID\tINTEGER")


    delta_log.log(') ' + ' IN ' + tabspace)
    all_log.log(') ' + ' IN ' + tabspace)
    his_log.log(') ' + ' IN ' + tabspace)

    tmp_primary_list = primary_list[:]
    tmp_primary_list.append('EFF_DT')

    tmp_primary_str = ','.join(tmp_primary_list)

    primary_str = ','.join(primary_list)

    if tmp_primary_str:
        all_log.log("Partitioning Key ("+tmp_primary_str+") Using Hashing")

    if primary_str:
        delta_log.log("Partitioning Key ("+primary_str+") Using Hashing")
        his_log.log("Partitioning Key ("+primary_str+") Using Hashing")
        
    delta_log.log("Compress Yes;")
    all_log.log("Compress Yes;")
    his_log.log("Compress Yes;")

    if table_comment:
        delta_log.log("Comment on Table "+delta_tablename+" is "+'\''+table_comment+'\';')
        all_log.log("Comment on Table "+tablename+" is "+'\''+table_comment+'\';')
        his_log.log("Comment on Table "+his_tablename+" is "+'\''+table_comment+'\';')

    for field_code,field_comment in field_code_dict.items():
        delta_log.log("Comment on Column "+delta_tablename+'.'+field_code+'\tis \''+field_comment+'\';')
        all_log.log("Comment on Column "+tablename+'.'+field_code+'\tis \''+field_comment+'\';')
        his_log.log("Comment on Column "+his_tablename+'.'+field_code+'\tis \''+field_comment+'\';')
        # print(field_code)

    all_log.log('')
    all_log.log('--------------------------------------------------')
    all_log.log('-- Create Index '+tablename+'_'+local_time+'_1')
    all_log.log('--------------------------------------------------')
    all_log.log('create  Index '+tablename+'_'+local_time+'_1')
    all_log.log('   on '+tablename)
    all_log.log('   (END_DT)    Allow Reverse Scans;\n')

    all_log.log('--------------------------------------------------')
    all_log.log('-- Create Index '+tablename+'_'+local_time+'_2')
    all_log.log('--------------------------------------------------')
    all_log.log('create  Index '+tablename+'_'+local_time+'_2')
    all_log.log('   on '+tablename)
    all_log.log('   (JOB_SEQ_ID)    Allow Reverse Scans;\n')

    if primary_str:
        delta_log.log('')
        delta_log.log('--------------------------------------------------')
        delta_log.log('-- Create Index '+delta_tablename)
        delta_log.log('--------------------------------------------------')
        delta_log.log('create  Index '+delta_tablename)
        delta_log.log('   on '+delta_tablename)
        delta_log.log('   ('+primary_str+')    Allow Reverse Scans;\n')

    his_log.log('')
    his_log.log('--------------------------------------------------')
    his_log.log('-- Create Index '+his_tablename)
    his_log.log('--------------------------------------------------')
    his_log.log('create  Index '+his_tablename)
    his_log.log('   on '+his_tablename)
    his_log.log('   (NEW_JOB_SEQ_ID)    Allow Reverse Scans;\n')

    if primary_str:
        all_log.log('')
        all_log.log("-- DDL Statements for Primary Key on Table ")
        all_log.log("ALTER TABLE "+tablename+" ADD PRIMARY KEY ("+tmp_primary_str+");")
        all_log.log('')
    
    print(tablename+":generate ddl success ")
    generate_ap_sql_init(field_code_list, delta_tablename, tablename, date)
    generate_ap_sql(field_code_list,delta_tablename,tablename,date,his_tablename,primary_list)

## 处理字段更新
def deal_column_update(table, newdate, olddate, update_list, is_primary, new_column_dict, old_type, new_type, column):
    his_tablename = "ODSHIS."+table.replace('.','_')
    delta_tablename = "DELTA."+table.replace('.','_')
    field_code_list = []
    if is_primary == 1: ## 主键表
        old_primary = old_type.split(',')[3]
        new_primary = new_type.split(',')[3]
        if old_primary == "N" and new_primary == "Y": ## 非主键变为主键

            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 重新生成ODS表 和ODSHIS表
            generate_ods_ddl(table,newdate,tabspace,tabletype)
            generate_his_ddl(table,newdate,tabspace,tabletype)


            ## rename ODSHIS, ODS表（表结尾加上”_yyyymmdd”)
            syscode,tablenm = table.split('.')
            his_syscode, his_tablenm = his_tablename.split('.')
            alter_table.log("--rename table")
            alter_table.log("rename table {} to {}_{}".format(table,tablenm,newdate))
            alter_table.log("rename table {} to {}_{}".format(his_tablename,his_tablenm,newdate))
            ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--add table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            # job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            job.log("UPDATE etl.job_metadata set init_flag='N' where job_nm='LD_ODS_%s_INIT';" %(table.replace('.','_')))
            job.log("UPDATE etl.job_metadata set init_flag='W' where job_nm='LD_ODS_%s';" %(table.replace('.','_')))
        elif old_primary == "Y" and new_primary == "N": ##主键变为非主键
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 重新生成ODS表 和ODSHIS表
            generate_ods_ddl(table,newdate,tabspace,tabletype)
            generate_his_ddl(table,newdate,tabspace,tabletype)

            ## rename ODSHIS, ODS表（表结尾加上”_yyyymmdd”)
            syscode,tablenm = table.split('.')
            his_syscode, his_tablenm = his_tablename.split('.')
            alter_table.log("--rename table")
            alter_table.log("rename table {} to {}_{}".format(table,tablenm,newdate))
            alter_table.log("rename table {} to {}_{}".format(his_tablename,his_tablenm,newdate))

            ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--del table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            # job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            job.log("UPDATE etl.job_metadata set init_flag='N' where job_nm='LD_ODS_%s_INIT';" %(table.replace('.','_')))
            job.log("UPDATE etl.job_metadata set init_flag='W' where job_nm='LD_ODS_%s';" %(table.replace('.','_')))
        elif old_primary == new_primary: ##字段属性变更
            alter_table.log("--property change")
            new_type_2 = new_type.split(',')[0]
            if new_type_2 == "CHARACTER":
                filed_line = 'CHARACTER(' +new_type.split(',')[1]+ ')'
            elif new_type_2 == "DECIMAL":
                filed_line = 'DECIMAL(' +new_type.split(',')[1]+','+new_type.split(',')[2]+ ')'
            elif new_type_2 == "VARCHAR":
                filed_line = 'VARCHAR(' +new_type.split(',')[1]+ ')'
            else:
                filed_line = new_type.split(',')[0]
            
            alter_table.log("alter table {} alter column {} set data type {};".format(delta_tablename,column,filed_line))
            alter_table.log("alter table {} alter column {} set data type {};".format(table,column,filed_line))
            alter_table.log("alter table {} alter column {} set data type {};".format(his_tablename,column,filed_line))

            print("--------reorg table(DELTA_TABLE,ODS_TABLE,ODSHIS_TABLE)---------")
            print(table+" execute sql: REORG TABLE "+table+';')
            print(table+" execute sql: REORG TABLE "+his_tablename+';')
            print(table+" execute sql: REORG TABLE "+delta_tablename+';')
            alter_table.log("REORG TABLE "+table+';\n')
            alter_table.log("REORG TABLE "+his_tablename+';\n')
            alter_table.log("REORG TABLE "+delta_tablename+';\n')

## 处理字段删除
def deal_column_del(table, newdate, olddate, del_list, is_primary, old_column_dict):
    field_code_list=[]
    syscode,tablenm = table.split('.')
    his_tablename = "ODSHIS."+table.replace('.','_')
    delta_tablename = "DELTA."+table.replace('.','_')
    if is_primary == 1: ## 主键表
        print(table+ ' has primary_key')
        ## 2.判断添加字段中是否含有主键
        primary_list=[]
        # read_me_log.log("变更下发表：")
        
        for key in del_list:
            try:
                primary_list.append(old_column_dict[key].split(',')[3])
            except KeyError:
                for key,value in old_column_dict.items():
                    print(key,value)
            print("del column and column_type "+key+":"+old_column_dict[key])
            sql = "select field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{0}' and tab_code = '{1}' and field_code='{2}' and CHANGE_DATE='{3}'".format(syscode,tablenm,key,olddate)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            read_me_log.log("主键表删除字段及类型: "+table+"."+key+":"+old_column_dict[key]+'\t'+rows[0][0]+'\n')

        if "Y" not in primary_list: ##主键表删除非主键字段
            # read_me_log.log("DEL COLLUMN(NOT PRIMARY KEY):")
            ## 重新生成DELTA表
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 4.重新生成AP_INIT
            # read_me_log.log(table+" --------重新生成AP_INIT---------")
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

        else: #(主键表删除主键字段)
            # read_me_log.log("\nDEL COLUMN(PRIMARY KEY):")
            print("primary_key in columns")
            ## 重新生成DELTA表
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 重新生成ODS表 和ODSHIS表
            generate_ods_ddl(table,newdate,tabspace,tabletype)
            generate_his_ddl(table,newdate,tabspace,tabletype)

            ## rename ODSHIS, ODS表（表结尾加上”_yyyymmdd”)
            syscode,tablenm = table.split('.')
            his_syscode, his_tablenm = his_tablename.split('.')
            alter_table.log("--rename table")
            alter_table.log("rename table {} to {}_{}".format(table,tablenm,newdate))
            alter_table.log("rename table {} to {}_{}".format(his_tablename,his_tablenm,newdate))

            ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--del table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            # job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            job.log("UPDATE etl.job_metadata set init_flag='N' where job_nm='LD_ODS_%s_INIT';" %(table.replace('.','_')))
            job.log("UPDATE etl.job_metadata set init_flag='W' where job_nm='LD_ODS_%s';" %(table.replace('.','_')))



    else: ## 无主键表
        print(table+ ' not has primary_key')
        for key in del_list:
            sql = "select field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{0}' and tab_code = '{1}' and field_code='{2}' and CHANGE_DATE='{3}'".format(syscode,tablenm,key,olddate)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            read_me_log.log("无主键表删除字段及类型: "+table+"."+key+":"+old_column_dict[key]+'\t'+rows[0][0]+'\n')
        # read_me_log.log("无主键表删除字段:")
            ## 重新生成DELTA表
        tabletype="add column"
        generate_delta_ddl(table,newdate,tabspace,tabletype)

        ## 4.重新生成AP_INIT
        # read_me_log.log(table+" --------重新生成AP_INIT---------")
        syscode,tablenm = table.split('.')
        sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
        cursor_dw.execute(sql)
        rows = cursor_dw.fetchall()
        column_primary_key_list = []
        for row in rows:
            field_code,primary_key = row
            field_code_list.append(field_code)
            if primary_key == "Y":
                column_primary_key_list.append(field_code)

        delta_tablename = "DELTA."+table.replace('.','_')

        generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

        ## 5.重新生成AP
        print(table+" --------重新生成AP---------")
        his_tablename = "ODSHIS."+table.replace('.','_')
        generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

## 处理增加字段
def deal_column_add(table, newdate, add_list, is_primary, new_column_dict):
    ## 1.判断是否为主键表
    field_code_list=[]
    delta_tablename = "DELTA."+table.replace('.','_')
    his_tablename = "ODSHIS."+table.replace('.','_')
    syscode,tablenm = table.split('.')
    if is_primary == 1: #主键表
        print(table+ ' has primary_key')
        ## 2.判断添加字段中是否含有主键
        primary_list=[]
        # read_me_log.log("变更下发表：")
        
        for key in add_list:
            primary_list.append(new_column_dict[key].split(',')[3])
            print("add column and column_type "+key+":"+new_column_dict[key])
            sql = "select field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{0}' and tab_code = '{1}' and field_code='{2}' and CHANGE_DATE='{3}'".format(syscode,tablenm,key,newdate)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            read_me_log.log("主键表增加字段及类型: "+table+"."+key+":"+new_column_dict[key]+'\t'+rows[0][0]+'\n')

        if "Y" not in primary_list: #主键表添加非主键字段
            print("columns are not primary_key")
            # read_me_log.log("ADD COLLUMN(NOT PRIMARY KEY) IN TABLE WITH PRIMARY:")
            ## 3.重新生成DELTA表
            # read_me_log.log("第一步: 重新生成DELTA表(文件位于对应日期目录下)")
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 4.重新生成AP_INIT
            # read_me_log.log(table+" --------重新生成AP_INIT---------")
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            ## 6.alter table 增加ODS, ODSHIS表字段
            print("--------alter table 增加ODS, ODSHIS表字段---------")
            add_str = ','.join("\'%s\'" %x for x in add_list)
            sql = "select src_stm_id,tab_code,field_code from DSA.ORGIN_TABLE_DETAIL where src_stm_id='{0}' and tab_code = '{1}' and change_date='{2}' and field_code in ({3}) order by cast(column_id as int)".format(syscode,tablenm,newdate,add_str)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            # f = open(newdate+"/table_add_column.sql",'a')
            for row in rows:
                src_stm_id,tab_code,field_code = row
                # print(field_code)
                # print(new_column_dict[field_code])

                filed_type,length,precsn,primary_flag = new_column_dict[field_code].split(',')
                if filed_type == "CHARACTER":
                    filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
                elif filed_type == "DECIMAL":
                    filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
                elif filed_type == "VARCHAR":
                    filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
                else:
                    filed_line = field_code +'\t'+ filed_type

                # print(filed_line)
                print(table+" execute sql: alter table "+ table + " add column " + filed_line+";")
                print(table+" execute sql: alter table "+ his_tablename + " add column " + filed_line+";")
                alter_table.log("alter table "+ table + " add column " + filed_line+";\n")
                alter_table.log("alter table "+ his_tablename + " add column " + filed_line+";\n")
            
            # f.close()
            ## 7.reorg table(ODS_TABLE,ODSHIS_TABLE) 
            # f = open(newdate+"/reorg_table.sql",'a')
            print("--------reorg table(ODS_TABLE,ODSHIS_TABLE)---------")
            print(table+" execute sql: REORG TABLE "+table+';')
            print(table+" execute sql: REORG TABLE "+his_tablename+';')
            alter_table.log("REORG TABLE "+table+';\n')
            alter_table.log("REORG TABLE "+his_tablename+';\n')
            # f.close()

            ## 8.更新调度语句
            # f = open(newdate+"/job.sql",'a')
            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--add table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            
            # f.close()

            ## 9.生成临时AP作业
            print("--------生成临时AP作业---------")
            f = open(newdate+"/AP/AP_ODS_"+table.replace('.','_')+"_YATOPUPDATE.SQL",'w')
            primary_str = ','.join(column_primary_key_list)
            add_str = ','.join(add_list)
            add_and_str =','.join("T.%s=S.%s" %(x,x) for x in add_list)
            primary_and_str = ' AND '.join("A.%s=B.%s" %(x,x) for x in column_primary_key_list)
            primary_and_str2 = ' AND '.join("T.%s=S.%s" %(x,x) for x in column_primary_key_list)

            null_str = ' = NULL,'.join(add_list)
            null_str = null_str + " = NULL"

            all_column = ','.join(field_code_list)

            f.write("--redo:\n")
            sql = "UPDATE {0} SET {1};\n".format(table,null_str)
            f.write(sql)

            sql = "DELETE FROM {0} WHERE JOB_SEQ_ID= (SELECT JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(table,newdate,table.replace('.','_'))
            f.write(sql)

            sql = "INSERT INTO {0}({1},EFF_DT,END_DT,JOB_SEQ_ID) select {1},EFF_DT,END_DT,JOB_SEQ_ID from {2} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{3}' AND JOB_NM ='AP_ODS_{4}');\n".format(table,all_column,his_tablename,newdate,table.replace('.','_'))
            f.write(sql)

            sql = "DELETE FROM {0} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n\n".format(his_tablename,newdate,table.replace('.','_'))
            f.write(sql)

            sql = "CREATE TABLE DELTA.{0}_YATOPUPDATE LIKE DELTA.{0};\n".format(table.replace('.','_'))
            f.write(sql)
            sql = "LOAD CLIENT FROM /etl/etldata/input/init/{0}/{1}_{0} of del replace into DELTA.{1}_YATOPUPDATE;\n".format(newdate,table.replace('.','_'))
            f.write(sql)
            
            sql = "MERGE INTO {0} T USING (SELECT {1},{2} FROM DELTA.{3}_YATOPUPDATE A WHERE NOT EXISTS (SELECT 1 FROM DELTA.{3} B WHERE {4})) S ON {5} AND T.END_DT='9999-12-31' WHEN MATCHED THEN UPDATE SET {6};\n".format(table,primary_str,add_str,table.replace('.','_'),primary_and_str,primary_and_str2,add_and_str)
            f.write(sql)
            sql = "DROP TABLE DELTA.{0}_YATOPUPDATE;\n".format(table.replace('.','_'))
            f.write(sql)
            f.close()
            # ftp_ap_sql('AP_ODS_'+table.replace('.','_')+'_UPDATE.SQL', newdate)

        else: # 主键表增加主键字段
            # read_me_log.log("ADD COLLUMN(PRIMARY KEY) IN TABLE WITH PRIMARY:")
            print("primary_key in columns")
            ## 重新生成DELTA表
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 重新生成ODS表 和ODSHIS表
            generate_ods_ddl(table,newdate,tabspace,tabletype)
            generate_his_ddl(table,newdate,tabspace,tabletype)


            ## rename ODSHIS, ODS表（表结尾加上”_yyyymmdd”)
            syscode,tablenm = table.split('.')
            his_syscode, his_tablenm = his_tablename.split('.')
            alter_table.log("--rename table")
            alter_table.log("rename table {} to {}_{}".format(table,tablenm,newdate))
            alter_table.log("rename table {} to {}_{}".format(his_tablename,his_tablenm,newdate))
            ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--add table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            # job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            job.log("UPDATE etl.job_metadata set init_flag='N' where job_nm='LD_ODS_%s_INIT';" %(table.replace('.','_')))
            job.log("UPDATE etl.job_metadata set init_flag='W' where job_nm='LD_ODS_%s';" %(table.replace('.','_')))



    else: #非主键表
        print(table+ ' not has primary_key')
        ## 判断添加字段中是否含有主键
        primary_list=[]
        # read_me_log.log("变更下发表：")
        
        for key in add_list:
            primary_list.append(new_column_dict[key].split(',')[3])
            print("add column and column_type "+key+":"+new_column_dict[key])
            sql = "select field_nm from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{0}' and tab_code = '{1}' and field_code='{2}' and CHANGE_DATE='{3}'".format(syscode,tablenm,key,newdate)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            read_me_log.log("非主键表增加字段及类型: "+table+"."+key+":"+new_column_dict[key]+'\t'+rows[0][0]+'\n')

        if "Y" not in primary_list: ## 非主键表增加非主键字段
            # read_me_log.log("ADD COLLUMN(NOT PRIMARY KEY) IN TABLE WITHOUT PRIMARY:")
            ## 重新生成DELTA表
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

             ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            ## alter table ODS ODSHIS
            ## 6.alter table 增加ODS, ODSHIS表字段
            print("--------alter table 增加ODS, ODSHIS表字段---------")
            add_str = ','.join("\'%s\'" %x for x in add_list)
            sql = "select src_stm_id,tab_code,field_code from DSA.ORGIN_TABLE_DETAIL where src_stm_id='{0}' and tab_code = '{1}' and change_date='{2}' and field_code in ({3}) order by cast(column_id as int)".format(syscode,tablenm,newdate,add_str)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            # f = open(newdate+"/table_add_column.sql",'a')
            for row in rows:
                src_stm_id,tab_code,field_code = row
                # print(field_code)
                # print(new_column_dict[field_code])

                filed_type,length,precsn,primary_flag = new_column_dict[field_code].split(',')
                if filed_type == "CHARACTER":
                    filed_line = field_code +'\t'+'CHARACTER(' +length+ ')'
                elif filed_type == "DECIMAL":
                    filed_line = field_code +'\t'+'DECIMAL(' +length+','+precsn+ ')'
                elif filed_type == "VARCHAR":
                    filed_line = field_code +'\t'+'VARCHAR(' +length+ ')'
                else:
                    filed_line = field_code +'\t'+ filed_type

                # print(filed_line)
                print(table+" execute sql: alter table "+ table + " add column " + filed_line+";")
                print(table+" execute sql: alter table "+ his_tablename + " add column " + filed_line+";")
                alter_table.log("alter table "+ table + " add column " + filed_line+";\n")
                alter_table.log("alter table "+ his_tablename + " add column " + filed_line+";\n")
            
            # f.close()
            ## 7.reorg table(ODS_TABLE,ODSHIS_TABLE) 
            # f = open(newdate+"/reorg_table.sql",'a')
            print("--------reorg table(ODS_TABLE,ODSHIS_TABLE)---------")
            print(table+" execute sql: REORG TABLE "+table+';')
            print(table+" execute sql: REORG TABLE "+his_tablename+';')
            alter_table.log("REORG TABLE "+table+';\n')
            alter_table.log("REORG TABLE "+his_tablename+';\n')

            ## 8.更新调度语句
            # f = open(newdate+"/job.sql",'a')
            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--add table columns")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            

            # f.close()

            ## 9.生成临时AP作业
            print("--------生成临时AP作业---------")
            f = open(newdate+"/AP/AP_ODS_"+table.replace('.','_')+"_YATOPUPDATE.SQL",'w')

            primary_str = ','.join(field_code_list)
            add_str = ','.join(add_list)
            add_and_str =','.join("T.%s=S.%s" %(x,x) for x in add_list)
            primary_and_str = ' AND '.join("A.%s=B.%s" %(x,x) for x in field_code_list)
            primary_and_str2 = ' AND '.join("T.%s=S.%s" %(x,x) for x in field_code_list)

            null_str = ' = NULL,'.join(add_list)
            null_str = null_str + " = NULL"

            all_column = ','.join(field_code_list)

            f.write("--redo:\n")
            sql = "UPDATE {0} SET {1};\n".format(table,null_str)
            f.write(sql)

            sql = "DELETE FROM {0} WHERE JOB_SEQ_ID= (SELECT JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n".format(table,newdate,table.replace('.','_'))
            f.write(sql)

            sql = "INSERT INTO {0}({1},EFF_DT,END_DT,JOB_SEQ_ID) select {1},EFF_DT,END_DT,JOB_SEQ_ID from {2} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{3}' AND JOB_NM ='AP_ODS_{4}');\n".format(table,all_column,his_tablename,newdate,table.replace('.','_'))
            f.write(sql)

            sql = "DELETE FROM {0} WHERE NEW_JOB_SEQ_ID= (SELECT  JOB_SEQ_ID FROM ETL.JOB_LOG WHERE TO_CHAR(DATA_PRD,'yyyymmdd')='{1}' AND JOB_NM ='AP_ODS_{2}');\n\n".format(his_tablename,newdate,table.replace('.','_'))
            f.write(sql)


            sql = "CREATE TABLE DELTA.{0}_YATOPUPDATE LIKE DELTA.{0};\n".format(table.replace('.','_'))
            f.write(sql)
            sql = "LOAD CLIENT FROM /etl/etldata/input/init/{0}/{1}_{0} of del replace into DELTA.{1}_YATOPUPDATE;\n".format(newdate,table.replace('.','_'))
            f.write(sql)
            
            sql = "MERGE INTO {0} T USING (SELECT {1},{2} FROM DELTA.{3}_YATOPUPDATE A WHERE NOT EXISTS (SELECT 1 FROM DELTA.{3} B WHERE {4})) S ON {5} AND T.END_DT='9999-12-31' WHEN MATCHED THEN UPDATE SET {6};\n".format(table,primary_str,add_str,table.replace('.','_'),primary_and_str,primary_and_str2,add_and_str)
            f.write(sql)
            sql = "DROP TABLE DELTA.{0}_YATOPUPDATE;\n".format(table.replace('.','_'))
            f.write(sql)
            f.close()


        else: #非主键表增加主键字段
            # read_me_log.log("ADD COLLUMN(PRIMARY KEY) IN TABLE WITHOUT PRIMARY:")
            print("primary_key in columns")
            ## 重新生成DELTA表
            tabletype="add column"
            generate_delta_ddl(table,newdate,tabspace,tabletype)

            ## 重新生成ODS表 和ODSHIS表
            generate_ods_ddl(table,newdate,tabspace,tabletype)
            generate_his_ddl(table,newdate,tabspace,tabletype)


            ## rename ODSHIS, ODS表（表结尾加上”_yyyymmdd”)
            syscode,tablenm = table.split('.')
            his_syscode, his_tablenm = his_tablename.split('.')
            alter_table.log("--rename table")
            alter_table.log("rename table {} to {}_{}".format(table,tablenm,newdate))
            alter_table.log("rename table {} to {}_{}".format(his_tablename,his_tablenm,newdate))
            ## 重新生成AP 和 AP_INIT
            syscode,tablenm = table.split('.')
            sql = "select field_code,case when primary_key_flag='' then 'N' else 'Y' end from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' order by cast(column_id as int)".format(tablenm,newdate,syscode)
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()
            column_primary_key_list = []
            for row in rows:
                field_code,primary_key = row
                field_code_list.append(field_code)
                if primary_key == "Y":
                    column_primary_key_list.append(field_code)

            delta_tablename = "DELTA."+table.replace('.','_')

            generate_ap_sql_init(field_code_list,delta_tablename,table,newdate)

            ## 5.重新生成AP
            print(table+" --------重新生成AP---------")
            his_tablename = "ODSHIS."+table.replace('.','_')
            generate_ap_sql(field_code_list,delta_tablename,table,newdate,his_tablename,column_primary_key_list)

            print("--------更新调度语句---------")
            # print("Update set etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            job.log("--add table columns")
            # job.log("Update etl.job_metadata set init_flag='N' where job_nm ='AP_ODS_%s_UPDATE';" %(table.replace('.','_')))
            # job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='AP_ODS_%s_YATOPUPDATE';" %(table.replace('.','_')))
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='UNCOMPRESS_INIT';")
            job.log("UPDATE ETL.JOB_METADATA SET INIT_FLAG='N' WHERE JOB_NM ='FTP_DOWNLOAD_INIT';")
            job.log("UPDATE etl.job_metadata set init_flag='N' where job_nm='LD_ODS_%s_INIT';" %(table.replace('.','_')))
            job.log("UPDATE etl.job_metadata set init_flag='W' where job_nm='LD_ODS_%s';" %(table.replace('.','_')))

# 获取字段级差异
def deal_columns(need_tables,olddate,newdate):
    for table in need_tables:
        # print(table)
        # tablelist = ["IMBS.T_CKSY_SZMX","IMBS.MID71_T_SIGN","IMBS.T_CKSY_NSZMX","IMBS.T_ENTR","IMBS.T_ZJSSB_BAT","IMBS.T_TXZF_AUTO","IMBS.T_CKSY_SZMX","IMBS.T_YHT_PRT_FMT","IMBS.T_TXZF_AUTO","IMBS.T_ZJWATER_JRNL","IMBS.T_RFMQF_JRNL","IMBS.T_ENTR","IMBS.T_ADB_JRNL","IMBS.T_ZJZF_JS_JRNL","IMBS.T_ELE_BAT","IMBS.T_KQMQF_BAT"]
        # if table != "CORE.BBFMAULM":
        #     continue

        ## 判断是否为主键表
        syscode, tablenm = table.split('.')
        sql = "select count(1) from DSA.ORGIN_TABLE_DETAIL where src_stm_id ='{2}' and tab_code = '{0}' and change_date='{1}' and primary_key_flag <>''".format(tablenm,olddate,syscode)
        cursor_dw.execute(sql)
        rows = cursor_dw.fetchall()
        for i in rows:
            if i[0] != 0:
                is_primary = 1
                # print(table+" has primary_key")
            else:
                is_primary = 0
                # print(table+" not has primary_key")

        old_column_dict, old_column_set = get_column_detail(table, olddate)
        # print(old_column_dict)
        # print(old_column_set)

        new_column_dict, new_column_set = get_column_detail(table, newdate)
        # print(new_column_dict)
        # print(new_column_set)

        

        add_set = new_column_set - old_column_set
        if add_set:
            # read_me_log.log(table + ' 新增字段: '+','.join(add_set))
            deal_column_add(table, newdate, list(add_set), is_primary, new_column_dict)

        del_set = old_column_set - new_column_set
        if del_set:
            print(table + ' del:', del_set)
            deal_column_del(table, newdate, olddate, list(del_set), is_primary, old_column_dict)

        common_set = old_column_set & new_column_set
        # reg = re.compile(',\d+$')

        if common_set:
            for i in common_set:
                old_type = old_column_dict.get(i)
                # old_type = re.sub(reg,'',old_type)
                new_type = new_column_dict.get(i)
                # new_type = re.sub(reg,'',new_type)
                if old_type != new_type:
                    print(table +' update:' + i +' old_type: '+old_type+' new_type:'+new_type)
                    read_me_log.log("\n字段属性更新: "+table+"."+i+' old_type: '+old_type+' new_type:'+new_type+'\n')
                    # update_list
                    deal_column_update(table, newdate, olddate, list(common_set), is_primary, new_column_dict, old_type, new_type, i)
        # break


## 新增模式名
def deal_add_schema(schema):
    compare_generate_log.log("新增模式名: %s" %schema)
    job.log("--add schema")

    sqls = ["INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('UNCOMPRESS_%s','DAY','CMD','L_ODSLD','uncompress.sh','%s $dateid','5','1','UNCOMPRESS_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" %(schema,schema,schema,schema,ip),
            "INSERT INTO ETL.JOB_METADATA (JOB_NM,SCHD_PERIOD,JOB_TP,LOCATION,JOBCMD,PARAMS,PRIORITY,EST_WRKLD,MTX_GRP,INIT_FLAG,PPN_TSTMP,INIT_BATCH_NO,MAX_BATCH_NO,SRC_SYS_ID,JOB_DESC,SCHD_ENGIN_IP) VALUES ('FTP_DOWNLOAD_%s','DAY','FTP','FTP','154.125.31.81 sjff sjff 21 DOWNLOAD','/home/sjff/data/sdata/S-999000/%s/ADD/$dateid /etl/etldata/input/delta/%s %s_906000_$dateid_ADD.tar.Z %s_906000_$dateid_ADD.tar.Z','5','1','FTP_DOWNLOAD_%s','N',CURRENT TIMESTAMP,'1','1','%s','','%s');" % (
            schema, schema, schema, schema, schema, schema, schema, ip),
            "INSERT INTO ETL.JOB_SEQ (JOB_NM,PRE_JOB ,PPN_TSTMP) VALUES ('UNCOMPRESS_%s','FTP_DOWNLOAD_%s',CURRENT TIMESTAMP);" % (
            schema, schema)]

    for sql in sqls:
        job.log(sql)
        compare_generate_log.log(sql)

## 判断是否新增模式名
def deal_schema(olddate,newdate):
    sql="SELECT DISTINCT(SRC_STM_ID) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE = '{0}'".format(olddate)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    exists_schema_list = []
    excel_schema_list = []
    for i in rows:
        exists_schema_list.append(i[0].strip())

    sql="SELECT DISTINCT(SRC_STM_ID) FROM DSA.ORGIN_TABLE_DETAIL WHERE CHANGE_DATE = '{0}'".format(newdate)
    cursor_dw.execute(sql)
    rows = cursor_dw.fetchall()
    for i in rows:
        excel_schema_list.append(i[0].strip())

    for schema in excel_schema_list:
        if schema not in exists_schema_list:
            deal_add_schema(schema)

if __name__=="__main__":
    try:
        with open("automatic.conf",'r') as f:
            data = f.readlines()

        conf_dict = {}
        for i in data:
            key,value = i.strip().split('=')
            conf_dict[key] = value

        
        DSN=conf_dict["DSN"]
        tabspace=conf_dict["TABSPACE"]
        ip=conf_dict["IP"]
        edwdb=conf_dict["edwdb"]
        edwuser=conf_dict["edwuser"]
        edwpwd=conf_dict["edwpwd"]
        dwmmdb=conf_dict["dwmmdb"]
        dwmmuser=conf_dict["dwmmuser"]
        dwmmpwd=conf_dict["dwmmpwd"]

        con = pyodbc.connect("DSN=%s" % DSN)
        # print(con)
        cursor_dw = con.cursor()

        datestring = input("请输入一个日期或者是要对比的两个日期,以逗号隔开,若不需要,请输入Enter跳过:")

        if not datestring:
            sql = "select distinct change_date from DSA.ORGIN_TABLE_DETAIL order by change_date"
            cursor_dw.execute(sql)
            rows = cursor_dw.fetchall()

            datelist = []
            for date in rows:
                datelist.append(date[0])

            datelist = datelist[-2:]
        else:
            datelist = datestring.split(',')

        ## 若没有直接退出
        if len(datelist) == 0:
            print("exists_file.txt is empty,exit...")
            input()
            sys.exit()
        ## 若存在一个日期,则全为新增表
        elif len(datelist) == 1:
            print("only one date,all tables will be definited on insert")
            datelist.insert(0,'19000101')

        print(datelist)

        with open("content.txt","r") as f:
            data = f.read()

        reg = re.compile("/S-999000/ODS/ALL/"+datelist[1]+"/")
        result = re.findall(reg, data)
        if result:
            print("这次变更为全量")
            quanliang_flag = 1
        else:
            quanliang_flag = 0

        if not os.path.isdir(datelist[1]):
            os.mkdir(datelist[1])
        if not os.path.isdir(datelist[1]+'/AP'):
            os.makedirs(datelist[1]+'/AP')

        ## 日志处理
        delta_log = Logger('delta_log', datelist[1]+'/delta_tables.ddl')
        all_log = Logger('all_log', datelist[1]+'/ods_tables.ddl')
        his_log = Logger('his_log', datelist[1]+'/his_tables.ddl')

        delta_log.log("connect to {} user {} using {};".format(edwdb,edwuser,edwpwd))
        all_log.log("connect to {} user {} using {};".format(edwdb,edwuser,edwpwd))
        his_log.log("connect to {} user {} using {};".format(edwdb,edwuser,edwpwd))

        read_me_log = Logger('read_me_log', datelist[1]+'/README.txt')
        compare_generate_log = Logger("compare_generate_log", datelist[1]+'/compare_generate.log')
        # table_add_column = Logger('table_add_column', datelist[1]+'/table_add_column.sql')
        # reorg_table = Logger('reorg_table', datelist[1]+'/reorg_table.sql')
        job = Logger('job', datelist[1]+'/job_schedule.SQL')

        job.log("connect to {} user {} using {};".format(dwmmdb,dwmmuser,dwmmpwd))
        # ap_job = Logger('ap_job', datelist[1]+'/ap_job.sql')
        alter_table = Logger('alter_table', datelist[1]+'/alter_table.sql')

        deal_schema(datelist[0],datelist[1])

        need_tables = get_differ_table(datelist[0], datelist[1])

        if need_tables:
            deal_columns(need_tables, datelist[0], datelist[1])


        if quanliang_flag == 1:
            job.log("UPDATE ETL.JOB_METADATA set INIT_FLAG ='Y' WHERE JOB_NM LIKE '%_INIT';")
            job.log("UPDATE ETL.JOB_METADATA set INIT_FLAG ='N' WHERE INIT_FLAG= 'W';")
            job.log("UPDATE ETL.JOB_METADATA set INIT_FLAG ='U' WHERE JOB_NM LIKE '%_YATOPUPDATE';")
            
        # result_file.close()
        # print("请确认变更情况是否正确，确认无误后请执行变更程序XXXX")
        read_me_log.log("请确认变更情况是否正确，确认无误后请执行变更程序sub_ftp.exe")
        answer = input("Enter Y to execute sub_ftp.exe or enter N to Exit:")
        if answer.upper() == "Y":
            subprocess.call("sub_ftp.exe")

    # ftp_job_schedule_ddl_sql(datelist[1])
    except Exception as e:
        print(sys.exc_info())
        # input()
        sys.exit()



