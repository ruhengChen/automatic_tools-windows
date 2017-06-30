# -*- coding=utf-8 -*-
import os
import rarfile
import re
import sys
import traceback
import xlrd
import pyodbc
import time
import datetime
import subprocess


def submit(date, table, filename):
    
    tablenm_list = []
    nrows = table.nrows
        # print(nrows)

    namelist = table.row_values(0)
    # print(namelist)

    if len(namelist) < 10:
        print("ERROR:" + filename)
        sys.exit(-1)
    if not namelist[0] == "源系统ID" and namelist[1] == "源文件/表代码" and namelist[2] == "源文件/表名称" and namelist[3] == "字段代码" and namelist[4] == "字段名称" and namelist[5] == "字段数据类型" and namelist[6] == "字段长度" and namelist[7] == "字段精度" and namelist[8] == "是否进入标准化" and namelist[9] == "现在是否使用（包括不下发）" and namelist[10] == "主键标识":
        print("ERROR:" + filename)
        sys.exit(-1)

    column_id = 1
    for i in range(1,nrows):
        # print(i)
        valuelist = table.row_values(i)
        if not tablenm_list:
            tablenm_list.append(valuelist[1])
            # continue

        if tablenm_list[0] != valuelist[1]:
            tablenm_list[0] = valuelist[1]
            column_id = 1
            # continue

        valuelist.insert(0,date)
        valuelist.insert(5,column_id)

        valuelist = valuelist[:18]
        valuelist_len = len(valuelist)

        i = 15   # 去除了BI列
        while True:
            if i < valuelist_len:
                valuelist[13] = str(valuelist[13]) + str(valuelist[i])
                i += 1
            else:
                break
        valuelist = valuelist[:14]
        valuelist.append(filename)
        # print(valuelist)
        if valuelist[4].strip().upper() == "ALL" or valuelist[4] == "":
            # print("skip ALL ...")
            continue

        for i in range(len(valuelist)):
            try:
                valuelist[i] = int(valuelist[i])
            except ValueError:
                pass
            valuelist[i] = str(valuelist[i]).upper().replace('\'', '\'\'')

        if valuelist[9] == "":
            valuelist[9] = 0
        tmp = ','.join(["\'%s\'" %x for x in valuelist])

        sql = "insert into DSA.ORGIN_TABLE_DETAIL values("+tmp+")"
        print(sql)
        cursor_dw.execute(sql)
        cursor_dw.commit()
        column_id += 1
        # cursor_dw.close()

def un_rar(file_name):  
    """unrar zip file"""  
    rar = rarfile.RarFile(file_name)
    if os.path.isdir(file_name + "_files"):
        pass
    else:
        os.mkdir(file_name + "_files")
    os.chdir(file_name + "_files")
    rar.extractall()
    rar.close()

def get_existlist():
    os.chdir("../tools")
    with open("exists_file.txt",'r') as f:
        exist_filelist = f.readlines()

    for i in range(0,len(exist_filelist)):
        exist_filelist[i] = exist_filelist[i].strip()
    return exist_filelist

def update_existlist(filename):
    os.chdir("../tools")
    with open("exists_file.txt",'a') as f:
        f.write(filename.lstrip('../')+'\n')

def transfer_excel_db2(filename, zone):
    excel_list = os.listdir(filename + "_files")
    os.chdir(filename + "_files")
    print(os.getcwd())
    date = re.split('-|\.',filename.lstrip("../"))[1]

    for excelname in excel_list:
        if excelname != "全量数据过滤表清单.xls" and excelname != "数据标准化－码表.xls":
            # print(excelname)
            data = xlrd.open_workbook(excelname)

            print(excelname)
            table = data.sheet_by_name("标准化字段")
            print("标准化字段")

            submit(date, table, excelname)

        if excelname == "数据标准化拆分-中间业务-发布版.xls":
            try:
                table = data.sheet_by_name(zone+"地区标准化")
                print(zone+"地区标准化")
                submit(date,table,excelname)
            except ValueError:
                print("不存在"+zone+"地区标准化")
            except xlrd.biffh.XLRDError:
                print("不存在"+zone+"地区标准化")
    #     print(excel_list)


if __name__=="__main__":

    try:

        ## 获取当前时间,
        local_time = datetime.datetime.now()
        print(local_time)

        if not os.path.exists("automatic.log"):
            print("automatic.log is not exists,exit...")
            input()
            sys.exit(-1)
        with open("automatic.log","r") as f:
            data = f.readlines()

        if not data:
            print("automatic.log is empty,exit...")
            input()
            sys.exit(-1)

        spider_list = []
        reg = ".*spider success"
        reg = re.compile(reg)

        for spider_data in data:
            result = re.findall(reg, spider_data)
            if result:
                spider_list.append(result)

        last_spider = spider_list[-1][0]
        print(last_spider)
        # print(last_spider[0])
        set_time, item = str(last_spider).split(" :: ")

        set_time = datetime.datetime.strptime(set_time,"%Y-%m-%d %H:%M:%S")
        print(set_time)

        today=local_time - datetime.timedelta(hours=6)

        # while True:
        #     ## 若当前时间小于爬虫时间并且当前时间为今日时间,则开始解压文件
        #     if local_time > set_time and set_time > today:
        #         write_str = " :: loadin success\n"
        #         break
        #         # with open("automatic.log","a") as f :
        #         #     f.write(local_time.strftime("%Y-%m-%d %H:%M:%S") + " :: loadin success with data\n")
        #     else:
        #         print("invalid time,waiting,the time must after %s and before %s" %(today, local_time))
        #         time.sleep(300)
        
        if not os.path.exists("automatic.conf"):
            print("automatic.conf is not exists,exit...")
            input()
            sys.exit(-1)
        
        ## 读取配置信息
        with open("automatic.conf",'r',encoding='utf-8') as f:
            data = f.readlines()

        conf_dict = {}
        for i in data:
            key,value = i.strip().split('=')
            conf_dict[key] = value

        zone=conf_dict["ZONE"]
        DSN=conf_dict["DSN"]

        try:
            con = pyodbc.connect("DSN=%s" %DSN)
            cursor_dw = con.cursor()
        except Exception:
            print("DB2 connect error ,check your DSN,exit...")
            input()
            sys.exit(-1)

        print("ZONE is: "+zone)
        ## 判断需要的表是否存在
        sql = "select tabname from syscat.tables where tabschema = 'DSA' and tabname = 'ORGIN_TABLE_DETAIL'"
        cursor_dw.execute(sql)
        rows = cursor_dw.fetchall()

        ## 若不存在,建表
        if not rows:
            print("the table DSA.ORGIN_TABLE_DETAIL is not exits,creating")
            sql = "CREATE TABLE DSA.ORGIN_TABLE_DETAIL (CHANGE_DATE VARCHAR(10) NOT NULL,SRC_STM_ID VARCHAR(10) NOT NULL,TAB_CODE VARCHAR(50) NOT NULL,TAB_NM VARCHAR(200),FIELD_CODE VARCHAR(50) NOT NULL,COLUMN_ID VARCHAR(10),FIELD_NM VARCHAR(500),DATA_TP VARCHAR(20),LENGTH VARCHAR(20),PRECSN VARCHAR(20),IS_FIELD_DIST VARCHAR(10),IS_FIELD_USED_DESC VARCHAR(2048),PRIMARY_KEY_FLAG VARCHAR(100),COMMENT VARCHAR(20480),FILE_NAME VARCHAR(2048))"
            cursor_dw.execute(sql)
            cursor_dw.commit()

        file_list = []
        filename_list = os.listdir("..")

        ## 判断文件是否已经解析
        exist_filelist = get_existlist()

        
        for filename in filename_list:
            if (os.path.splitext(filename))[1] == ".rar":
                file_list.append(filename)

        ## 若文件未解压,开始解压
        for filename in file_list:
            if filename not in exist_filelist:
                filename = "../"+ filename
                un_rar(filename)

        write_str = " :: loadin success with data\n"
        ## 判断是否解压成功,成功就入库
        load_flag = 0
        for filename in file_list:
            if filename not in exist_filelist:
                filename = "../"+ filename
                excel_list = os.listdir(filename + "_files")
                if not excel_list:
                    print(filename + "  unrar error or package is empty,sysexit...")
                    write_str = " :: loadin error\n"
                    with open("automatic.log","a") as f :
                        f.write(local_time.strftime("%Y-%m-%d %H:%M:%S") + write_str)
                    # input()
                    sys.exit()

                try:
                    load_flag = 1
                    transfer_excel_db2(filename,zone)  ## 入库
                # break
                    update_existlist(filename)

                except Exception as e:
                    print("loadin error")
                    print(e)
                    input()
                    sys.exit(-1)

        if load_flag == 1:
            ## 需要不需要执行下一个程序
            answer = input("Enter Y to execute compare_generate.exe or enter N to exit:")
            if answer.upper() == "Y":
                subprocess.call("compare_generate.exe")

        with open("automatic.log","a") as f :
                f.write(local_time.strftime("%Y-%m-%d %H:%M:%S") + write_str)
        print("loadin success")

        
            
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        input()
        sys.exit(-1)