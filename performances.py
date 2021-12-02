
import sys
import subprocess
import csv
machines_perf=[]
for i in range(1,31):
    out_clean = subprocess.check_output([sys.executable, "INF727_Systemes_repartis/Clean.py", "INF727_Systemes_repartis/amazon_file-1.txt","--distributed"])
    out_deploy = subprocess.check_output([sys.executable, "INF727_Systemes_repartis/Deploy.py",f"--{i}"])
    out_master = subprocess.check_output([sys.executable, "INF727_Systemes_repartis/Master.py", "INF727_Systemes_repartis/amazon_file-1.txt","--distributed"])
    #print(out_master)
    result=out_master.decode("utf-8") .split("\n")[:-1]
    result.append(i)
    machines_perf.append(result)


#machines_perf=[['2.8136749267578125', '1.6066241264343262', '17.093552112579346', '1.4826302528381348', '10.800810098648071', '3.3729631900787354', '3.717811107635498', '12.51427698135376', '53.66592311859131',0],
#['2.8136749267578125', '1.6066241264343262', '17.093552112579346', '1.4826302528381348', '10.800810098648071', '3.3729631900787354', '3.717811107635498', '12.51427698135376', '53.66592311859131',1]]

#print(machines_perf)
csv_file = "INF727_Systemes_repartis/perf.csv"
csv_columns = ['split_input','create_mkdir','send_splits','map','shuffle','reduce','send_reduce','result','total','nb_machines']


with open(csv_file, 'w', newline='') as myfile:
    writer = csv.writer(myfile)
    writer.writerow(csv_columns)
    writer.writerows(machines_perf)


