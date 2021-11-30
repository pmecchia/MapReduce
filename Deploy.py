import subprocess
import sys
import re



def ssh(command,machines):
    listproc = []
    #timer=5
    login="pmecchia-20"
    delete_list=[]
    for ip in machines:
        proc = subprocess.Popen(["ssh",login+"@"+ip,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=10)
            code = listproc[i].returncode
            if code!=0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
                if code==255: ## nodename or servname not known
                    delete_list.append(i)
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            print(f"{machines[i]} deleted from list")
            del machines[i]
    for i in delete_list:
        del machines[i]
    return machines

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



if __name__ == '__main__':
    nbMachines = int(re.findall(r'\d+',sys.argv[1])[0])
    ip_list=[]
    for i in range(1,nbMachines + 1):
            machine=f"tp-4b01-{i:02d}"
            ip_list.append(machine)
    ip_list=ssh("hostname",ip_list) #keep only working machines

    with open("INF727_Systemes_repartis/list_ip.txt", "w") as f:
        for index,machine in enumerate(ip_list):
            if index < len(ip_list):
                f.write(machine+"\n")
            else:
                f.write(machine)



    ip_list=ssh("mkdir -p /tmp/pmecchia-20",ip_list)
    scp("INF727_Systemes_repartis/Slave.py", "/tmp/pmecchia-20",ip_list)
