import subprocess
import sys
import re



def ssh(command,machines):
    listproc = []
    #timer=5
    login="pmecchia-20"

    for ip in machines:
        proc = subprocess.Popen(["ssh",login+"@"+ip,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code!=0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            del ip_list[i]

    return ip_list

def scp(localPath,distantPath,machines):
    listproc = []
    #timer=200
    login="pmecchia-20"
    for ip in machines:
        proc = subprocess.Popen(["scp",localPath,login+"@"+ip+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code !=0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")


#file = open('INF727_Systemes_repartis/list_ip.txt', 'r')
#ip_list = file.read().splitlines()
nbMachines = int(re.findall(r'\d+',sys.argv[1])[0])

print(nbMachines)

ip_list=[]
start=5
with open("INF727_Systemes_repartis/list_ip.txt", "w") as f:
    for i in range(start, start+nbMachines + 1):
        machine=f"tp-4b01-{i:02d}"
        ip_list.append(machine)
        if i < start+nbMachines:
            f.write(machine+"\n")
        else:
            f.write(machine)


ip_list=ssh("mkdir -p /tmp/pmecchia-20",ip_list)
scp("INF727_Systemes_repartis/Slave.py", "/tmp/pmecchia-20",ip_list)
ssh("ls /tmp/pmecchia-20",ip_list)


#ssh("hostname")
