import sys
import subprocess
import csv
import glob

csv_columns = ['split_input','create_mkdir','send_splits','map','shuffle','reduce','send_reduce','result','valid_result','total','static_res','nb_machines']

for file in glob.glob("input_files/*.txt"):
    machines_perf=[]
    out_master_static = subprocess.check_output([sys.executable, "Master.py", file,"--local"])

for i in range(2,31):
    out_clean = subprocess.check_output([sys.executable, "Clean.py"])
    out_deploy = subprocess.check_output([sys.executable, "Deploy.py",f"--{i}"])
    out_master = subprocess.check_output([sys.executable, "Master.py", file,"--distributed"])

    result=out_master.decode("utf-8") .split("\n")[:-1] #algo results

    result.append(out_master_static.decode("utf-8") .split("\n")[0]) #static result
    result.append(i) #number of machines

    machines_perf.append(result)



csv_file = "perf_results/"+file.split("/")[1][:-4]+".csv"

with open(csv_file, 'w', newline='') as myfile:
    writer = csv.writer(myfile)
    writer.writerow(csv_columns)
    writer.writerows(machines_perf)
    

