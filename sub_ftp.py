import paramiko
import os
import json
import sys
import re
import traceback


class Logger:        
    def __init__(self, logName, logFile):
        self._logger = logging.getLogger(logName)
        handler = logging.FileHandler(logFile,mode='w')
        self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def log(self, msg):
        if self._logger is not None:
            self._logger.info(msg)
            

def get_sftp_connect(retries=3):
    properties_dict = {}
    if not os.path.exists('ftp.conf'):
        job.log("ftp.conf not exists..")
        sys.exit(-1)

    with open('ftp.conf') as f:
        data = f.readlines()
    for i in data:
        properties,value = i.rstrip('\n').split('=')
        properties_dict[properties] = value

    HOSTNAME=properties_dict['HOSTNAME']
    PORT=int(properties_dict['PORT'])
    USERNAME=properties_dict['USERNAME']
    PASSWROD=properties_dict['PASSWROD']

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(hostname=HOSTNAME, port=PORT, username=USERNAME, password=PASSWROD)
    except:
        if retries > 0:
            print("FTP connect Error,retry...")
            return get_sftp_connect(retries=retries - 1)
        else:
            print("FTP connect Error...")
            sys.exit(-1)

    return ssh


def transport(retries=3):
    properties_dict = {}
    if not os.path.exists('ftp.conf'):
        job.log("ftp.conf not exists..")
        sys.exit(-1)

    with open('ftp.conf') as f:
        data = f.readlines()
    for i in data:
        properties,value = i.rstrip('\n').split('=')
        properties_dict[properties] = value

    HOSTNAME=properties_dict['HOSTNAME']
    PORT=int(properties_dict['PORT'])
    USERNAME=properties_dict['USERNAME']
    PASSWROD=properties_dict['PASSWROD']

    try:
        transport = paramiko.Transport(HOSTNAME,PORT)
        transport.connect(username=USERNAME,password=PASSWROD)
    except:
        if retries > 0:
            print("FTP transport Error,retry")
            return transport(retries=retries - 1)
        else:
            print("FTP connect Error...")
            sys.exit(-1)
    
    sftp = paramiko.SFTPClient.from_transport(transport)

    # 将location.py 上传至服务器 /tmp/test.py
    return sftp

    # sftp.put('/tmp/location.py', '/tmp/test.py')
    # # 将remove_path 下载到本地 local_path
    # sftp.get('remove_path', 'local_path')
      
    # transport.close()
def get_exists_date():
    ssh = get_sftp_connect()
    exists_datelist = []
    stdin, stdout, stderr = ssh.exec_command('ls -d /etl/etldata/script/yatop_update/* |grep -P [0-9]+')

    result = stdout.read()
    result_list = result.decode().split('\n')
    for result in result_list:
        if result:
            exists_datelist.append(result.split('/')[-1])
    return exists_datelist

def ftp_job_schedule_ddl_sql(date,up_load_file_list):
    ssh = get_sftp_connect()
    exists_datelist = get_exists_date()
    print("exists_datelist:", exists_datelist)
    if date not in exists_datelist:
        cmd = "mkdir -p /etl/etldata/script/yatop_update/"+ date
        print(cmd)
        stdin,stdout,stderr = ssh.exec_command(cmd)
        result = stderr.read().decode('gb18030')
        print(result)

    sftp = transport()

    for file in up_load_file_list:
        print(os.path.join('.',date,file))
        sftp.put(os.path.join('.',date,file), os.path.join('/etl/etldata/script/yatop_update/'+date+'/'+file))
        print("upload "+file+" success")

def ftp_ap_sql(ap_sql_name,date,ssh):
    
    ### 判断远程是否存在,存在的话进行备份
    stdin,stdout,stderror = ssh.exec_command("ls /etl/etldata/script/odssql/"+ap_sql_name)
    result = stdout.read().decode('gb18030')

    cmd = "ls /etl/etldata/script/yatop_update/"+date+"/backup/"+ap_sql_name
    stdin2,stdout2,stderror2 = ssh.exec_command(cmd)
    result2 = stdout2.read().decode('gb18030')

    if result2:
        print("backup exists " + ap_sql_name)

    else:
        if result:
            stdin,stdout,stderror = ssh.exec_command("cp /etl/etldata/script/odssql/"+ap_sql_name+" /etl/etldata/script/yatop_update/"+date+"/backup")
            if not stderror.read().decode('gb18030'):
                print("backup success "+ ap_sql_name)
        else:
            print("no need backup " + ap_sql_name)

    sftp = transport()
    sftp.put('./'+date+'/AP/'+ap_sql_name, '/etl/etldata/script/odssql/'+ap_sql_name)
    print("upload "+ap_sql_name+" success")
    # sftp.close()

def deal_log(reg,result,date):
    patten = re.compile(reg)
    errors = re.findall(patten, result)
    if errors:
        ssh = get_sftp_connect()
        cmd = "cat /etl/etldata/script/yatop_update/"+date+"/"+reg+".log |grep -A 1 -P \"SQL[0-9]+N\""
        stdin,stdout,stderror = ssh.exec_command(cmd)
        result = stdout.read().decode('gb18030')
        print(result)
        print()
        print()

def execute(file_name):
    cmd = "db2 -tvf /etl/etldata/script/yatop_update/"+date+"/"+file_name
    print(cmd)
    stdin,stdout,stderror = ssh.exec_command(cmd)
    result = stdout.read().decode('gb18030')
    print(result)

    with open("execute.txt",'a') as f:
        f.write(cmd)
        f.write(result)

    error = stderror.read().decode('gb18030')
    print(error)


    reg = 'SQL\d+.*?SQLSTATE=\d+'
    reg = re.compile(reg,re.S)
    error = re.findall(reg,result)
    if error:
        print("execute "+file_name+" error: %s" %[x for x in error])
        input()
        sys.exit()

if __name__ == "__main__":

    try:
        
        ## 判断确认此日期大于job_log的最大批量日期  这个需要从job_log表中读取日期后比较
        with open("exists_file.txt","r") as f:
            datelist = f.readlines()
            datelist.sort()
            for i in range(0,len(datelist)):
                datelist[i] = datelist[i].lstrip("标准化文档-").rstrip(".rar\n")
        date = datelist[-1]
        print(date)
        
        job = Logger('job.log', date+'/job.log')
        
        up_load_file_list = ["job_schedule.SQL","delta_tables.ddl","alter_table.sql","his_tables.ddl","ods_tables.ddl"]

        ## 上传所有的ddl
        ftp_job_schedule_ddl_sql(date,up_load_file_list)

        ssh = get_sftp_connect()
        ## 判断是否存在目录,不存在就新建目录
        stdin,stdout,stderror = ssh.exec_command("ls /etl/etldata/script/yatop_update/"+date+"/odssql")
        result=stdout.read().decode('gb18030')
        if not result:
            ssh.exec_command("mkdir -p /etl/etldata/script/yatop_update/"+date+"/odssql")
        
        ## 创建备份目录
        ssh.exec_command("mkdir -p /etl/etldata/script/yatop_update/"+date+"/backup")

        ## 上传所有的apsql
        APlist = os.listdir(date+"/AP")
        for ap_sql_name in APlist:
            ftp_ap_sql(ap_sql_name,date,ssh)


        with open("automatic.conf",'r') as f:
            data = f.readlines()

        conf_dict = {}
        for i in data:
            key,value = i.strip().split('=')
            conf_dict[key] = value

        edwdb=conf_dict["edwdb"]
        edwuser=conf_dict["edwuser"]
        edwpwd=conf_dict["edwpwd"]
        dwmmdb=conf_dict["dwmmdb"]
        dwmmuser=conf_dict["dwmmuser"]
        dwmmpwd=conf_dict["dwmmpwd"]


        print("edwdb", edwdb)
        print("edwuser", edwuser)
        print("edwpwd", edwpwd)
        print("dwmmdb", dwmmdb)
        print("dwmmuser", dwmmuser)
        print("dwmmpwd", dwmmpwd)


        ## 备份原表
        backup_tables = []
        ssh = get_sftp_connect()
        stdin,stdout,stderror = ssh.exec_command("cat /etl/etldata/script/yatop_update/"+date+"/alter_table.sql |grep -i -E 'alter|drop'")
        result = stdout.read().decode('gb18030')
        stdin,stdout,stderror = ssh.exec_command("cat /etl/etldata/script/yatop_update/"+date+"/delta_tables.ddl |grep -i -E 'drop'")
        result2 = stdout.read().decode('gb18030')

        reg = '[d|a][r|l][o|t][p|e]r? table (.*?) .*;'
        reg = re.compile(reg,re.I)
        backup_tables = re.findall(reg,result)
        backup_tables.extend(re.findall(reg,result2))
        print(backup_tables)
        
        reg = re.compile("CREATE TABLE.*")

        for table in backup_tables:
            cmd = "ls /etl/etldata/script/yatop_update/{}/backup/{}.ddl.bak".format(date,table)
            stdin,stdout,stderror = ssh.exec_command(cmd)
            result = stdout.read().decode('gb18030')

            if result:
                print("backup exists" + table)
            else:
                schema, name = table.split('.')

                cmd = "db2look -d {} -i {} -w {} -z {} -e -t {} -nofed -o /etl/etldata/script/yatop_update/{}/backup/{}.ddl.bak".format(edwdb,edwuser,edwpwd, schema,name,date,table)
                print(cmd)
                stdin,stdout,stderror = ssh.exec_command(cmd)
                result = stdout.read().decode('gb18030')
                error = stderror.read().decode('gb18030')
                print(result)
                print(error)

                stdin,stdout,stderror = ssh.exec_command("cat /etl/etldata/script/yatop_update/{}/backup/{}.ddl.bak".format(date,table))
                result = stdout.read().decode('gb18030')
                # print(result)

                is_error = re.findall(reg,result)
                if not is_error:
                    print("backup error: "+table+" please check table is exists...")
                    input()
                    sys.exit()  

        # print(a)
        ## 备份ETL调度表

        stdin,stdout,stderror = ssh.exec_command("ls /etl/etldata/script/yatop_update/{}/backup/JOB_METADATA.del".format(date))
        result = stdout.read().decode('gb18030')
        if not result:
            print("export JOB_METADATA...")
            cmd = 'db2 connect to {} user {} using {} && db2 "export to /etl/etldata/script/yatop_update/{}/backup/JOB_METADATA.del of del select * from ETL.JOB_METADATA"'.format(dwmmdb,dwmmuser,dwmmpwd, date)
            print(cmd)
            stdin,stdout,stderror = ssh.exec_command(cmd)
            result = stdout.read().decode('gb18030')

            print(result)
            error = stderror.read().decode('gb18030')
            print(error)

            reg = re.compile("Number of rows exported.*")
            is_error = re.findall(reg, result)
            if not is_error:
                print("export JOB_METADATA error")
                input()
                sys.exit()


        print("load JOB_METADATA...")
        cmd ='db2 connect to {} user {} using {} && db2 "load from /etl/etldata/script/yatop_update/{}/backup/JOB_METADATA.del of del modified by identityoverride replace into ETL.JOB_METADATA"'.format(dwmmdb,dwmmuser,dwmmpwd, date)
        print(cmd)
        stdin,stdout,stderror = ssh.exec_command(cmd)
        result = stdout.read().decode('gb18030')

        error = stderror.read().decode('gb18030')
        print(error)

        reg = re.compile("SQL\d+")
        is_error = re.findall(reg, result)

        print(result)
        if not is_error:
            print("load JOB_METADATA error")
            input()
            sys.exit()


        stdin,stdout,stderror = ssh.exec_command("ls /etl/etldata/script/yatop_update/{}/backup/JOB_SEQ.del".format(date))
        result = stdout.read().decode('gb18030')
        if not result:
            print("export JOB_SEQ")
            cmd = 'db2 connect to {} user {} using {} && db2 "export to /etl/etldata/script/yatop_update/{}/backup/JOB_SEQ.del of del select * from ETL.JOB_SEQ"'.format(dwmmdb,dwmmuser,dwmmpwd, date)
            print(cmd)
            stdin,stdout,stderror = ssh.exec_command(cmd)
            result = stdout.read().decode('gb18030')

            print(result)

            error = stderror.read().decode('gb18030')
            print(error)

            reg = re.compile("Number of rows exported.*")
            is_error = re.findall(reg, result)
            if not is_error:
                print("export JOB_SEQ error")
                input()
                sys.exit()

        print("load JOB_SEQ")
        cmd = 'db2 connect to {} user {} using {} && db2 "load from /etl/etldata/script/yatop_update/{}/backup/JOB_SEQ.del of del replace into ETL.JOB_SEQ"'.format(dwmmdb,dwmmuser,dwmmpwd, date)
        print(cmd)
        stdin,stdout,stderror = ssh.exec_command(cmd)
        result = stdout.read().decode('gb18030')

        print(result)
        error = stderror.read().decode('gb18030')
        print(error)

        reg = re.compile("SQL\d+")
        is_error = re.findall(reg, result)
        if not is_error:
            print("load JOB_SEQ error")
            input()
            sys.exit()

        execute_list = ["delta_tables.ddl","ods_tables.ddl","his_tables.ddl","alter_table.sql","job_schedule.SQL"]

        for file in execute_list:
            execute(file)



        ssh.close()

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        print(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        input()
        sys.exit()


