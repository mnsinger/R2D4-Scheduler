##from datetime import datetime
##from datetime import date
##from datetime import timedelta
import datetime
import ibm_db
import re
import getpass
import mskcc
import pypyodbc
import xlsxwriter
import subprocess
import os
import sys
import dateutil
import shutil

# THIS CURRENTLY RUNS IN PYTHON 3.4

###########################
#        TIME DATA        #
###########################

now = datetime.datetime.now()
print("now: {}".format(now))
now_day_of_month = int(now.strftime('%d'))
now_day_of_week = int(now.strftime('%w'))
now_hour = now.strftime('%H')
now_minute = now.strftime('%M')

now_string = now.strftime('%Y%m%d-%H%M%S')
now_string_long = now.strftime('%B %d, %Y at %H:%M %p')

###########################
#       CONNECTION        #
###########################

input_file_1 = '../properties.txt'
f_in = open(input_file_1, 'r')
properties_dict = {}
for line in f_in:
    properties_dict[line.partition('=')[0]] = line.partition('=')[2].strip()
f_in.close()

connection_idb = ibm_db.connect('DATABASE=DB2P_MF;'
                     'HOSTNAME=ibm3270;'
                     'PORT=3021;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["idb_service_uid1"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["idb_service_pwd1"]).decode("latin-1")), '', '')

connection_darwin = ibm_db.connect('DATABASE=DVPDB01;'
                     'HOSTNAME=pidvudb1;'
                     'PORT=51013;'
                     'PROTOCOL=TCPIP;'
                     'UID={0};'.format(properties_dict["darwin_uid"]) +
                     'PWD={0};'.format(mskcc.decrypt(properties_dict["darwin_pwd"]).decode("latin-1")), '', '')

connection_sql_server = pypyodbc.connect("Driver={{SQL Server}};Server={};Database={};Uid={};Pwd={};".format(
                    "PS23A,61692",
                    "DEDGPDLR2D2",
                    properties_dict["sqlserver_ps23a_uid"],
                    mskcc.decrypt(properties_dict["sqlserver_ps23a_pwd"]).decode("latin-1")
                    )
                )

connection_sql_server_func = pypyodbc.connect("Driver={{SQL Server}};Server={};Database={};Uid={};Pwd={};".format(
                    "PS23A,61692",
                    "DEDGPDLR2D2",
                    properties_dict["sqlserver_ps23a_uid"],
                    mskcc.decrypt(properties_dict["sqlserver_ps23a_pwd"]).decode("latin-1")
                    )
                )

connection_sql_server_log = pypyodbc.connect("Driver={{SQL Server}};Server={};Database={};Uid={};Pwd={};".format(
                    "PS23A,61692",
                    "DEDGPDLR2D2",
                    properties_dict["sqlserver_ps23a_uid"],
                    mskcc.decrypt(properties_dict["sqlserver_ps23a_pwd"]).decode("latin-1")
                    )
                )

###########################
#        CONSTANTS        #
###########################

input_file_1 = ''.format(now)
output_file_1 = ''
output_log_file_1 = 'output.log'.format(now)
output_log_file_2 = 'debug.log'.format(now)
bufsize = 1

###########################
#        FUNCTIONS        #
###########################

def check_run_now(row):

    days_of_month_set = set()
    days_of_week_set = set()

    if row['days_of_month']:
        for day_of_month in row['days_of_month'].split(','):
            day_of_month = int(day_of_month.strip())
            days_of_month_set.add(day_of_month)
    else:
        day_col_names = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
        for day_col_name in day_col_names:
            if row[day_col_name]:
                days_of_week_set.add(day_col_names.index(day_col_name))
            
    #f_2.write("check_run_now. days_of_month_set: {}, days_of_week_set: {}\n".format(days_of_month_set, days_of_week_set))
    #f_2.write("enabled: {}, start_time: {}, row[minute]: {}\n".format(row["enabled"], row["start_time"], row["minute"]))
        
    print(row)

    # make the value into a time object
    row["start_time"] = datetime.datetime.strptime(row["start_time"], "%Y-%m-%d %H:%M:%S")

    print("enabled: {}, start_time: {}, row[minute]: {}".format(row["enabled"], row["start_time"], row["minute"]))

    # if enabled and run_minute = now minute
    if row["enabled"] and row["start_time"] <= now and row["minute"] == int(now_minute):

        #f_2.write("enabled and minutes are equal.\n".format(days_of_month_set, days_of_week_set))
        #f_2.write("checking if it runs today. today_day_of_week: {}, today_day_of_month: {}.\n".format(now_day_of_week, now_day_of_month))

        # does job run today?
        if now_day_of_month in days_of_month_set or now_day_of_week in days_of_week_set:

            print("job runs today")

            #f_2.write("yes job runs today - checked days of month and week.\n".format(days_of_month_set, days_of_week_set))
                
            #############################
            ###        HOURLY         ###
            #############################
            
            if row["interval"] == "HOURLY":
                print(row["interval"])

                if row["interval_n"] == 1:
                    #f_2.write("runs hourly. RETURNING TRUE.\n".format(days_of_month_set, days_of_week_set))
                    return True

                # if start_time hour:minute is earlier in the same day than run_hour and run_minute - no need to change start_time
                if row["start_time"] <= row["start_time"].replace(hour=row["hour"], minute=row["minute"]):
                    tdiff = now.replace(second=0, microsecond=0) - row["start_time"].replace(hour=row["hour"], minute=row["minute"], second=0, microsecond=0)
                # if start_time hour:minute takes place later in the day than run_hour and run_minute - it means that first run took place the following day
                else:
                    tdiff = now.replace(second=0, microsecond=0) - (row["start_time"] + datetime.timedelta(days=1)).replace(hour=row["hour"], minute=row["minute"], second=0, microsecond=0) 

                #f_2.write("checking tdiff. tdiff: {}.\n".format(tdiff))

                tdiff_hours = tdiff.seconds/3600
                                   
                #f_2.write("checking if it runs every n hours. tdiff_hours: {}, divisor: {}.\n".format(tdiff_hours, int(row["interval_n"])))

                # run every n hours 
                if tdiff_hours % int(row["interval_n"]) == 0:
                    #f_2.write("runs every {} hours. RETURNING TRUE.\n".format(row["interval_n"]))
                    return True

            #############################
            ###        DAILY          ###
            #############################
            
            if row["interval"] == "DAILY":
                print("now_hour: {}, run_hour: {}, now_minute: {}, run_minute: {}".format(now_hour, row["hour"], now_minute, row["minute"]))
                if int(row["hour"]) == int(now_hour) and int(row["minute"]) == int(now_minute):
                    print("run now!!!\n")
                    return True
                
            #############################
            ###       BI-WEEKLY       ###
            #############################

            # already know minutes are equal, day (of month or week) are equal
            
            if row["interval"] == "BI_WEEKLY" and int(row["hour"]) == int(now_hour):
                # GET FIRST TIME TO RUN AFTER SCHEDULE START
                # GET NUMBER OF DAYS / WEEKS SINCE FIRST RUN
                # IF MOD 2 == 0 THEN RUN

                # CONFUSING - MONDAY:0, TUESDAY:1 ... SUNDAY:6
                sched_start_day_of_week = row["start_time"].weekday()

                first_run = None
                first_run_day_of_week = next(iter(days_of_week_set)) - 1
                if first_run_day_of_week == -1:
                    first_run_day_of_week = 6
                
                # IF FIRST RUN WAS AFTER SCHED, THEN NEED TO ADD DAYS
                if first_run_day_of_week > sched_start_day_of_week:
                    first_run = row["start_time"] + datetime.timedelta(first_run_day_of_week - sched_start_day_of_week)
                # IF FIRST RUN WAS BEFORE SCHED, THEN NEED TO PUSH FORWARD 7 BUT STEP BACK HOWEVER MANY DAYS DIFF
                elif first_run_day_of_week < sched_start_day_of_week:
                    first_run = row["start_time"] + datetime.timedelta(7 - (sched_start_day_of_week - first_run_day_of_week))
                # IF SAME DAY, THEN NEED TO CHECK HOURS, MINUTES
                elif first_run_day_of_week == sched_start_day_of_week:
                    if row["start_time"] <= row["start_time"].replace(hour=row["hour"], minute=row["minute"]):
                        first_run = row["start_time"]
                    else:
                        first_run = row["start_time"] + datetime.timedelta(7)
                
                delta = now - first_run
                days_diff = delta.days
                weeks_diff = int(days_diff / 7)

                print("first_run: {}, first_run_day_of_week: {}, sched_start_day_of_week: {}, days_diff: {}, weeks_diff: {}, now_hour: {}, run_hour: {}, now_minute: {}, run_minute: {}".format(first_run, first_run_day_of_week, sched_start_day_of_week, days_diff, weeks_diff, now_hour, row["hour"], now_minute, row["minute"]))
                if weeks_diff % 2 == 0:
                    print("run now!!!\n")
                    return True
                
            #############################
            ###       MONTHLY         ###
            #############################

            # already know minutes are equal, day (of month or week) are equal
            
            if row["interval"] == "MONTHLY" and int(row["hour"]) == int(now_hour):
                #days_of_month = [m.start() for m in re.finditer('1', row["days_of_month"])]
                print("days_of_month_set: {}, now day of month: {}".format(days_of_month_set, now_day_of_month))
                if now_day_of_month in days_of_month_set:
                    return True
            
            #############################
            ###       N MONTHS        ###
            #############################

            # already know minutes are equal, day (of month or week) are equal
            
            if row["interval"] == "N_MONTHS":

                now_step = now.replace(minute=0, second=0, microsecond=0)

                while now_step >= row["start_time"].replace(minute=0, second=0, microsecond=0):
                    if int(row["hour"]) == int(now_hour) and now_step.month == row["start_time"].month:
                        return True
                    now_step = now_step - dateutil.relativedelta.relativedelta(months=int(row["interval_n"]))

                #f_2.write("checking if it runs every n hours. tdiff_hours: {}, divisor: {}.\n".format(tdiff_hours, int(row["interval_n"])))

                
    else:
        return False

def remove_comments(sql_string):

    # remove all commnets that start with --
    regex = r'--.*?\n'
    sql_string = re.sub(regex, '', sql_string)

    # get list of all /* comments */
    # loop through list and replace them in sql only if it's not in the spreadsheet naming format
    regex = r'\/\*.*?\*\/'
    p = re.compile(regex)
    match_list = p.findall(sql_string)
    print(match_list)
    for match_string in match_list:
        regex = '\/\*\s*worksheet\s*:\s*([\w ]+)\s*\*\/'
        if not re.match(regex, match_string, re.IGNORECASE):
            sql_string = sql_string.replace(match_string, '')

    return sql_string

def create_excel_extract(row, extract_email_body=False):

    excel_output_filename = 'DataLine Results - {}-{}.xlsx'.format(row["project_code"], now.strftime('%Y%m%d-%H%M%S'))

    email_body = ""
    if extract_email_body:
        email_body = """<style>table, td { border-collapse: collapse; border: 1px solid black; margin: auto; text-align: center; }</style>"""

    sql = """

    SELECT *
    FROM [DEDGPDLR2D2].[dbo].[PROJECTS_R2D4_V]
    WHERE [Project Code] = '{}'
    
    """.format(row["project_code"])

    print(sql)
    
    cursor_func_1 = connection_sql_server_func.cursor()
    cursor_func_1.execute(sql)
    columns = [column[0] for column in cursor_func_1.description]
    row_raw_func_1 = cursor_func_1.fetchone()
    
    row_func_1 = row_to_dict(row_raw_func_1, columns)

    connection = connection_idb
    if row["database"] == 'DARWIN':
        connection = connection_darwin

    ## CHECK IF MULTIPLE SQL STATEMENTS / WORKSHEETS

    row_func_1["project sql"] = remove_comments(row_func_1["project sql"])

    row_func_1["project sql"] = row_func_1["project sql"].strip("; \n")

    sql_list = row_func_1["project sql"].split(";")

    print("sql to run: {} ".format(sql_list))

    rows_returned = False
    sheet_count = 1
    sheet_name = "Results"
    if len(sql_list) > 1:
        sheet_name = "Results 1"

    for sql in sql_list:

        print("running: {}".format(sql))

        # SHEET NAMES CAN BE IN FORMAT /* WORKSHEET: WORKSHEET NAME */
        regex = '\/\*\s*worksheet\s*:\s*([\w ]+)\s*\*\/'
        if re.search(regex, sql, re.IGNORECASE):
            sheet_name = re.search(regex, sql, re.IGNORECASE).group(1)

        try:
            stmt = ibm_db.exec_immediate(connection, sql)
            print("returned from ibm_db.execute(stmt)")

            db_dict = ibm_db.fetch_tuple(stmt)
            print("db_dict: {}".format(db_dict))
            print("returned from db_dict = ibm_db.fetch_both(stmt)")
            print("returned from database")

        except:
            db_dict = False
            email_body = 'Error'

        if db_dict != False:
            # CREATE WORKBOOK ONLY ON FIRST RUN
            if not rows_returned:
                workbook = xlsxwriter.Workbook(excel_output_filename)

            f_1.write("time: {}-{}, rows returned\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))
            
            f_1.write("time: {}-{}, created file: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), excel_output_filename))
            worksheet = workbook.add_worksheet(sheet_name)
            num_columns = ibm_db.num_fields(stmt)
            fmt = workbook.add_format({'bold': True})

            # OUTPUT HEADER ONLY ON FIRST SHEET
            if not rows_returned:
                worksheet.write(0, 0, "{}".format(row_func_1["project description"]), fmt)
                worksheet.write(1, 0, "DataLine Report {}".format(row_func_1["project code"]), fmt)
                worksheet.write(2, 0, "Produced on {}, by DataLine in Information Systems".format(now_string_long))
                worksheet.write(3, 0, 'See "Criteria" sheet for inclusion criteria')

            fmt = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1'})

            if extract_email_body:
                email_body += """<b>{}</b><br><br><table style="width:95%;">""".format(sheet_name)
                email_body += """<tr style="background:#528AE7;font-family:Tahoma;color:white;font-size:11.0pt;">"""
            else:
                email_body = row_func_1["project description"]


            # OUTPUT COLUMN NAMES
            for column_index in range(num_columns):
                worksheet.write(5, column_index, ibm_db.field_name(stmt, column_index), fmt)
                field_width = len(ibm_db.field_name(stmt, column_index))+3
                if 'MRN' in ibm_db.field_name(stmt, column_index) and field_width < 10:
                    field_width = 10
                worksheet.set_column(column_index, column_index, field_width)
                if extract_email_body:
                    email_body += """<td>{}</td>""".format(ibm_db.field_name(stmt, column_index))

            email_body += "</tr>"

            # OUTPUT QUERY RESULTS
            html_output = output_excel_rows(workbook, worksheet, stmt, db_dict, 6, return_html=extract_email_body)

            if extract_email_body:
                email_body += "{}</table><br><br>".format(html_output)

            print("returned from output_excel_rows")
            
            rows_returned = True

        sheet_count += 1

        # SOMEWHAT EDGE CASE. IF EXCEL IS MIX OF SPECIFIED SHEET NAMES AND GENERATED...
        if re.search('(\d+)$', sheet_name):
            sheet_name = "Results {}".format(int(re.search('(\d+)$', sheet_name).group(0)) + 1)
        else:
            sheet_name = "Results {}".format(sheet_count)

    if rows_returned:

        print("rows_returned")

        # create worksheet for data elements and criteria
        worksheet = workbook.add_worksheet('Criteria')
        options = { 'width': 800, 'height': 800, }

        worksheet.insert_textbox(2, 2, "Data Elements:\n {} \n\nCriteria:\n{}".format(row_func_1["data elements"], row_func_1["criteria"]), options)

        # create worksheet for sql
        worksheet = workbook.add_worksheet('SQL')
        options = { 'width': 1000, 'height': 1000, }
        worksheet.insert_textbox(2, 2, "SQL:\n\n{}".format(row_func_1["project sql"]), options)
            
        workbook.close()

    ibm_db.close

    cursor_func_1.close()
    connection_sql_server_func.close()

    return rows_returned, email_body, excel_output_filename

def create_email_extract(row):

    email_body = """<style>table, td { border-collapse: collapse; border: 1px solid black; margin: auto; text-align: center; }</style>"""

    sql = """

        SELECT *
        FROM [DEDGPDLR2D2].[dbo].[PROJECTS_R2D4_V]
        WHERE [Project Code] = '{}'
    
    """.format(row["project_code"])

    print(sql)
    
    cursor_func_1 = connection_sql_server_func.cursor()
    cursor_func_1.execute(sql)
    columns = [column[0] for column in cursor_func_1.description]
    row_raw_func_1 = cursor_func_1.fetchone()
    
    row_func_1 = row_to_dict(row_raw_func_1, columns)

    connection = connection_idb
    if row["database"] == 'DARWIN':
        connection = connection_darwin

    ## CHECK IF MULTIPLE SQL STATEMENTS / WORKSHEETS

    row_func_1["project sql"] = remove_comments(row_func_1["project sql"])

    row_func_1["project sql"] = row_func_1["project sql"].strip("; \n")

    sql_list = row_func_1["project sql"].split(";")

    print("sql to run: {} ".format(sql_list))

    rows_returned = False
    sheet_count = 1
    sheet_name = "Results"
    if len(sql_list) > 1:
        sheet_name = "Results 1"

    for sql in sql_list:

        print("running: {}".format(sql))

        # SHEET NAMES CAN BE IN FORMAT /* WORKSHEET: WORKSHEET NAME */
        regex = '\/\*\s*worksheet\s*:\s*([\w ]+)\s*\*\/'
        if re.search(regex, sql, re.IGNORECASE):
            sheet_name = re.search(regex, sql, re.IGNORECASE).group(1)

        try:
            stmt = ibm_db.exec_immediate(connection, sql)
            print("returned from ibm_db.execute(stmt)")

            db_dict = ibm_db.fetch_tuple(stmt)
            print("db_dict: {}".format(db_dict))
            print("returned from db_dict = ibm_db.fetch_both(stmt)")
            print("returned from database")

        except:
            db_dict = False
            email_body = 'Error'

        if db_dict != False:
            f_1.write("time: {}-{}, rows returned\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S')))

            email_body += """<b>{}</b><br><br><table style="width:95%;">""".format(sheet_name)
            
            num_columns = ibm_db.num_fields(stmt)

            # OUTPUT COLUMN NAMES
            email_body += """<tr style="background:#528AE7;font-family:Tahoma;color:white;font-size:11.0pt;">"""

            for column_index in range(num_columns):
                email_body += """<td>{}</td>""".format(ibm_db.field_name(stmt, column_index))

            email_body += "</tr>"

            # OUTPUT QUERY RESULTS
            if db_dict != False:
                email_body += return_html_rows(stmt, db_dict) + "</table><br><br>"
                rows_returned = True

            print("returned from return_html_rows")

        sheet_count += 1

        # SOMEWHAT EDGE CASE. IF EXCEL IS MIX OF SPECIFIED SHEET NAMES AND GENERATED...
        if re.search('(\d+)$', sheet_name):
            sheet_name = "Results {}".format(int(re.search('(\d+)$', sheet_name).group(0)) + 1)
        else:
            sheet_name = "Results {}".format(sheet_count)

    ibm_db.close

    cursor_func_1.close()
    connection_sql_server_func.close()

    return rows_returned, email_body

def run_python_script(row):
    errs = ''
    outs = ''

    ## DOESN'T MAKE SENSE TO LINK TO FILES ON NETWORK DRIVES:

    ## 1. properties.txt file contains passwords and such - we would have to store it on network drive
    ## 2. mskcc.py is a library module that contains functions to encrypt / decrypt and also send emails - we would have to put this on network drive also
    ## 3. instead of using subprocess.run, you'd have to use os.startfile
    ## 4. everything runs slower on network vs local file system

    ## SUMMARY: IT'S DOABLE TO PUT THIS ON NETWORK DRIVE BUT WOULD NEED PLANNING AND PERMISSION FIRST

    #os.startfile(r"H:/Clinical Systems/Data Administration/DataLine/Plans/IS/IS15906/IS15906.py")

    #cmd = ["python", r"C:/DataLine/{}/{}.py".format(row["project_code"], row["project_code"])]
    #cmd = ["python", r"H:\Clinical Systems\Data Administration\DataLine\Plans\{}\{}.py".format(row["project_code"], row["project_code"])]
    #cmd = ["python", r"H:/Clinical Systems/Data Administration/DataLine/Plans/IS/IS15906/IS15906.py"]

    #print("GOT DEPT INFO. ABBREV: {}, {}".format(row_func_1["abbrev"], row_func_1["directoryname"]))
    #subprocess.run(["python", '"\\pens62\sddshared\Clinical Systems\Data Administration\DataLine\Plans\IS\{}\{}.py"'.format(row["project_code"], row["project_code"])], cwd=r'd:\test\local')
    #subprocess.run(["python", '"C:\Users\singerm\Documents\local_python\Allowvals IDB Comparison\allowvals_rms_comparison.py"'], cwd='"C:\Users\singerm\Documents\local_python\Allowvals IDB Comparison"')

    ## THIS WORKS FOR 3.5 AND ABOVE ONLY (THIS CURRENTLY RUNS IN 3.4)
    ##subprocess.run(cmd, cwd=r"C:/DataLine/{}".format(row["project_code"]))

    directory_name = row["project_code"]
    for f in os.listdir(r'C:\DataLine\.'):
        regex = '^{}.*'.format(row["project_code"])
        if re.match(regex, f):
            directory_name = f

    script_path = r"C:/DataLine/{}/{}.py".format(directory_name, row["project_code"])
    #print("script_path: {}, cwd: {}".format(script_path, r"C:/DataLine/{}".format(directory_name)))
    args = [r"C:/Program Files/Python36/python.exe", script_path]
    #proc = subprocess.Popen(r"C:/Program Files/Python36/python.exe {}".format(script_path), cwd=r"C:/DataLine/{}".format(directory_name))
    proc = subprocess.Popen(args, cwd=r"C:/DataLine/{}".format(directory_name), stderr=subprocess.PIPE)
    try:
        outs, errs = proc.communicate(timeout=3000)
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()

    f_1.write("time: {}-{}, errs: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), str(errs)))
    
    #subprocess.run(cmd, cwd=r"H:/Clinical Systems/Data Administration/DataLine/Plans/IS/IS15906/IS15906.py", shell=False)
    #\\pens62\sddshared\Clinical Systems\Data Administration\DataLine\Plans\IS\IS15906
    #print("subprocess.run([{}])".format())

    return errs

def row_to_dict(row_raw, columns):
    row = {}
    x = 0
    for col in columns:
        row[col] = row_raw[x]
        x += 1
    return row

## only for fetch_both dictionary
def output_excel_column_headers(workbook, worksheet, in_dict, row, col):
    print("in output_excel_column_headers")
    fmt = workbook.add_format({'bold': True, 'bg_color': '#C5D9F1'})
    for key in in_dict.keys():
        if not isinstance(key, int):
            worksheet.write(row, col, key, fmt)
            col += 1
        
## only for fetch_tuple dictionary
def output_excel_rows(workbook, worksheet, in_stmt, in_dict, in_row_start, return_html=False):
    print("in output_excel_rows")
    #type_set = set()
    date_time_format = workbook.add_format({'num_format': 'mmm d, yyyy hh:mm AM/PM'})
    date_format = workbook.add_format({'num_format': 'mmm d, yyyy'})

    field_width = {}
    max_field_width = 250
    if in_dict != False:
        for c in range(len(in_dict)):
            field_width[c] = len(ibm_db.field_name(in_stmt, c))+3

    html_trs = ""
    while in_dict != False:
        html_trs += "<tr>"
        for c in range(len(in_dict)):
            if not in_dict[c] is None:
                if field_width[c] <= max_field_width and len(str(in_dict[c]).strip()) <= max_field_width and len(str(in_dict[c]).strip()) > field_width[c]:
                    field_width[c] = len(str(in_dict[c]).strip())+3

                if isinstance(in_dict[c], datetime.datetime) or isinstance(in_dict[c], datetime.date):
                    worksheet.write(in_row_start, c, str(in_dict[c]))
                    html_trs += "<td>{}</td>".format(str(in_dict[c]))
                    
                else:
                    worksheet.write(in_row_start, c, str(in_dict[c]).strip())
                    html_trs += "<td>{}</td>".format(str(in_dict[c]).strip())
                    
        html_trs += "</tr>"

        in_dict = ibm_db.fetch_tuple(in_stmt)
        in_row_start += 1

    for c in field_width:
        worksheet.set_column(c, c, field_width[c])

    print("finishing output_excel_rows")
    return html_trs

## only for fetch_tuple dictionary
def return_html_rows(in_stmt, in_dict):
    print("in return_html_rows")

    html_rows = ""
    while in_dict != False:
        html_rows += "<tr>"
        for c in range(len(in_dict)):
            if not in_dict[c] is None:
                if isinstance(in_dict[c], datetime.datetime):
                    html_rows += "<td>{}</td>".format(str(in_dict[c]))
                elif isinstance(in_dict[c], datetime.date):
                   html_rows += "<td>{}</td>".format(str(in_dict[c]))
                else:
                    html_rows += "<td>{}</td>".format(str(in_dict[c]).strip())
            else:
                html_rows += "<td></td>"
            
        in_dict = ibm_db.fetch_tuple(in_stmt)
        html_rows += "</tr>"

    print("finishing return_html_rows")
    return html_rows



#############################
###          MAIN         ###
#############################

##ID
##Project_Code
##Interval
##Start_Time
##Day_of_Week
##Day_of_Month
##Hour
##Minute
##Enabled

SQL = """

    select 'job' src, [id], dt.[Delivery_type_id], dt.[Delivery_type], [Project_Code], [Database], [Interval], Interval_N, [Start_Time],
        [Sunday],[Monday],[Tuesday],[Wednesday],[Thursday],[Friday],[Saturday],
        [Hour],[Minute],[Enabled],
        STUFF((SELECT distinct ',' + r.recipient from [DEDGPDLR2D2].dbo.SCHEDULER_RECIPIENTS r where s.id = r.scheduler_id FOR XML PATH('')),1,1,'') recipients,
        STUFF((SELECT distinct ',' + cast(d.day_of_month as varchar(8000)) from [DEDGPDLR2D2].dbo.SCHEDULER_DAYS_OF_MONTH d where s.id = d.scheduler_id FOR XML PATH('')),1,1,'') days_of_month,
        prereq, OPTION_VALUE_1 email_subject
    from [DEDGPDLR2D2].dbo.scheduler s
    left join [DEDGPDLR2D2].dbo.SCHEDULER_DELIVERY_TYPES dt on dt.delivery_type_id=s.delivery_type_id
    left join [DEDGPDLR2D2].dbo.SCHEDULER_OPTIONS on id=scheduler_id and OPTION_NAME = 'EMAIL_SUBJECT'

	union

    select 'run now' src, [id], dt.[Delivery_type_id], dt.[Delivery_type], [Project_Code], [Database], [Interval], Interval_N, [Start_Time],
        [Sunday],[Monday],[Tuesday],[Wednesday],[Thursday],[Friday],[Saturday],
        [Hour],[Minute],[Enabled],
        STUFF((SELECT distinct ',' + r.recipient from [DEDGPDLR2D2].dbo.SCHEDULER_RECIPIENTS r where s.id = r.scheduler_id FOR XML PATH('')),1,1,'') recipients,
        STUFF((SELECT distinct ',' + cast(d.day_of_month as varchar(8000)) from [DEDGPDLR2D2].dbo.SCHEDULER_DAYS_OF_MONTH d where s.id = d.scheduler_id FOR XML PATH('')),1,1,'') days_of_month,
        prereq, OPTION_VALUE_1  email_subject
    from [DEDGPDLR2D2].dbo.scheduler_run_now s
    left join [DEDGPDLR2D2].dbo.SCHEDULER_DELIVERY_TYPES dt on dt.delivery_type_id=s.delivery_type_id
    left join [DEDGPDLR2D2].dbo.SCHEDULER_OPTIONS on id=scheduler_id and OPTION_NAME = 'EMAIL_SUBJECT'

	union

    select 'prereq' src, [id], dt.[Delivery_type_id], dt.[Delivery_type], [Project_Code], [Database], [Interval], Interval_N, [Start_Time],
        [Sunday],[Monday],[Tuesday],[Wednesday],[Thursday],[Friday],[Saturday],
        [Hour],[Minute],[Enabled],
        STUFF((SELECT distinct ',' + r.recipient from [DEDGPDLR2D2].dbo.SCHEDULER_RECIPIENTS r where s.id = r.scheduler_id FOR XML PATH('')),1,1,'') recipients,
        STUFF((SELECT distinct ',' + cast(d.day_of_month as varchar(8000)) from [DEDGPDLR2D2].dbo.SCHEDULER_DAYS_OF_MONTH d where s.id = d.scheduler_id FOR XML PATH('')),1,1,'') days_of_month,
        prereq, OPTION_VALUE_1  email_subject
    from [DEDGPDLR2D2].dbo.scheduler_queue s
    left join [DEDGPDLR2D2].dbo.SCHEDULER_DELIVERY_TYPES dt on dt.delivery_type_id=s.delivery_type_id
    left join [DEDGPDLR2D2].dbo.SCHEDULER_OPTIONS on id=scheduler_id and OPTION_NAME = 'EMAIL_SUBJECT'
    
    order by src desc, id desc

"""

cursor = connection_sql_server.cursor()
cursor.execute(SQL)

SQL = "DELETE FROM dbo.scheduler_run_now".format()
print(SQL)
cursor_log = connection_sql_server_log.cursor()
cursor_log.execute(SQL)
cursor_log.commit()

f_1 = open(output_log_file_1, "a")
f_2 = open(output_log_file_2, "a")

row_raw = cursor.fetchone()
while row_raw is not None:
    #f_2.write("id: {}, time: {}-{}, row: {}\n".format(row_raw[0], now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), row_raw))

    columns = [column[0] for column in cursor.description]
    row = row_to_dict(row_raw, columns)

    prereq_met = True
    if row["src"] in ("run now", "prereq") or check_run_now(row):
        run_error = False
        run_notes = 'NULL'

        ## CHECK FOR PREREQ
        ## IF IT MEETS PREREQ - RUN IT
        ## ELSE INSERT INTO PREREQ QUEUE
        if row["prereq"]:
            sql = "select * from idb.availability where avl_appl = '{}' and avl_sts = 'Y' and date(avl_timestamp) = current date".format(row["prereq"])
            stmt = ibm_db.exec_immediate(connection_idb, sql)
            print("returned from ibm_db.execute(stmt)")

            try:
                db_dict = ibm_db.fetch_tuple(stmt)
                print("db_dict: {}".format(db_dict))
                print("returned from db_dict = ibm_db.fetch_both(stmt)")
                print("returned from database")

            except:
                db_dict = False

            ## JOB MEETS THE PREREQ
            if db_dict != False:
                ## DELETE FROM QUEUE
                SQL = "DELETE FROM dbo.scheduler_queue where id = '{}'".format(row["id"])
                print(SQL)
                cursor_log = connection_sql_server_log.cursor()
                cursor_log.execute(SQL)
                # what if job is in prereq_queue but this script takes more than a minute to run - there is the potential to run it more than once (unless you check the table to make sure it hasn't already been deleted) - *I THINK*
                rows_deleted = cursor_log.rowcount
                if row["src"] == "prereq" and rows_deleted == 0:
                    prereq_met = False
                    print("ROWS DELETED: {}. I WOULD RUN THIS JOB BUT IT HAS ALREADY BEEN RUN!!!".format(rows_deleted))
                cursor_log.commit()
            ## NEED TO CONTINUE WAITING
            else:
                SQL = "INSERT INTO dbo.scheduler_queue select * from dbo.scheduler where id = '{}' and not exists (select 'x' from dbo.scheduler_queue where id = '{}')".format(row["id"], row["id"])
                print(SQL)
                cursor_log = connection_sql_server_log.cursor()
                cursor_log.execute(SQL)
                cursor_log.commit()
                prereq_met = False

        if prereq_met:
            log_start_time = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
            SQL = "INSERT INTO dbo.scheduler_log (id, start_time, end_time, project_code) values ({}, '{}', NULL, '{}')".format(row["id"], log_start_time, row["project_code"])
            print(SQL)
            cursor_log = connection_sql_server_log.cursor()
            cursor_log.execute(SQL)
            cursor_log.commit()
            
            #f_2.write("row['delivery_type_id']\n".format(row['delivery_type_id']))
            f_1.write("time: {}-{}, row: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), row))

            ## EMAIL ATTACHMENT            
            if row["delivery_type_id"] == 1 or row["delivery_type_id"] == 3:
                #f_2.write("email attachment\n".format())
                f_1.write("time: {}-{}, delivery_type_id: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), row["delivery_type_id"]))
                if row["delivery_type_id"] == 1:
                    rows_returned, email_body, excel_output_filename = create_excel_extract(row)
                elif row["delivery_type_id"] == 3:
                    rows_returned, email_body, excel_output_filename = create_excel_extract(row, extract_email_body=True)
                if rows_returned:
                    email_subject = "DataLine Report - {}".format(row["project_code"])
                    if row["email_subject"]:
                        email_subject = row["email_subject"]
                    recipients_formatted = ["{}@mskcc.org".format(recipient.strip()) for recipient in row["recipients"].split(",")]
                    # CREATE ATTACHMENT
                    attachments = []
                    with open(excel_output_filename, 'rb') as f:
                        content = f.read()
                    attachments.append((excel_output_filename, content))
                    print("sending email attachment")
                    #mskcc.send_email(email_subject, email_body, recipients_formatted, attachments)
                    mskcc.send_mail("Data/Information Systems <data@mskcc.org>", ";".join(recipients_formatted), email_subject, email_body, attachments=[excel_output_filename], html=True)
                    print("finished sending email attachment")
                elif email_body == 'Error':
                    run_error = True
                    run_notes = "'Error'"

            ## EMAIL MESSAGE            
            if row["delivery_type_id"] == 2:
                #f_2.write("email attachment\n".format())
                f_1.write("time: {}-{}, delivery_type_id: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), row["delivery_type_id"]))
                rows_returned, email_body = create_email_extract(row)
                if rows_returned:
                    email_subject = "DataLine Report - {}".format(row["project_code"])
                    if row["email_subject"]:
                        email_subject = row["email_subject"]
                    recipients_formatted = ["{}@mskcc.org".format(recipient.strip()) for recipient in row["recipients"].split(",")]
                    #mskcc.send_email(email_subject, email_body, recipients_formatted, None, True)
                    mskcc.send_mail("Data/Information Systems <data@mskcc.org>", ";".join(recipients_formatted), email_subject, email_body, attachments=None, html=True)
                    print("finished sending email message")
                elif email_body == 'Error':
                    run_error = True
                    run_notes = "'Error'"

            ## PYTHON SCRIPT
            elif row["delivery_type_id"] == 4:
                #f_2.write("python script\n".format())
                errs = run_python_script(row)
                if len(errs) > 0:
                    f_1.write("time: {}-{}, errs: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), str(errs).replace("'","")[:500]))
                    run_error = True
                    #print(str(errs))
                    run_notes = "'" + str(errs).replace("'","")[:50] + "'"

            ## MOVE TO NETWORK DRIVE
            elif row["delivery_type_id"] == 5:
                f_1.write("time: {}-{}, delivery_type_id: {}\n".format(now_string, datetime.datetime.now().strftime('%Y%m%d-%H%M%S'), row["delivery_type_id"]))
                rows_returned, email_body, excel_output_filename = create_excel_extract(row)
                if rows_returned:
                    for network_drive in row["recipients"].split(","):
                        if network_drive[:-1] != "\\":
                            network_drive = "{}\\".format(network_drive.strip())
                        print(r"{}{}.xlsx".format(network_drive.strip(), row["project_code"]))
                        shutil.copyfile(excel_output_filename, r"{}DataLine Results - {}.xlsx".format(network_drive.strip(), row["project_code"]))
                    
                elif email_body == 'Error':
                    run_error = True
                    run_notes = "'Error'"

            log_end_time = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
            SQL = "UPDATE dbo.scheduler_log set end_time = '{}', run_notes = {} where id={} and start_time='{}'".format(log_end_time, run_notes, row["id"], log_start_time)
            print(SQL)
            cursor_log = connection_sql_server_log.cursor()
            cursor_log.execute(SQL)
            cursor_log.commit()
            
            cursor_log.close()

    row_raw = cursor.fetchone()


#f_2.write("\n".format())

cursor.close()
connection_sql_server.close()

