import os
import re
import time

current_time = time.time()

for f in os.listdir():
    if re.match("^DataLine Results - \w{2,3}\d{5}-\d{8}-\d{6}\.xlsx$", f):
        mod_time = os.path.getmtime(f)
        days_old = int((current_time - mod_time) / (24 * 3600))
        if days_old > 14:
            print("removing: {}".format(f))
            os.unlink(f)

excel_path = """C:\\R2D4\\eclipse-workspace\\DataLine\\WebContent\\WEB-INF\\excel"""

for f in os.listdir(excel_path):
    if re.match(".*xlsx$", f):
        path_to_file = excel_path + "\\" + f
        mod_time = os.path.getmtime(path_to_file)
        days_old = int((current_time - mod_time) / (24 * 3600))
        if days_old > 14:
            print("removing: {}".format(path_to_file))
            os.unlink(path_to_file)

email_path = """C:\\R2D4\\eclipse-workspace\\DataLine\\WebContent\\WEB-INF\\email_attachments"""

for f in os.listdir(email_path):
    path_to_file = email_path + "\\" + f
    mod_time = os.path.getmtime(path_to_file)
    days_old = int((current_time - mod_time) / (24 * 3600))
    if days_old > 14:
        print("removing: {}".format(path_to_file))
        os.unlink(path_to_file)
