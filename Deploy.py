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
                if code==255: ## nodename or servername not known
                    delete_list.append(i)
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(machines[i]+" timeout")
            #print(f"{machines[i]} deleted from list")
            delete_list.append(i)
            #del machines[i]
    for idx,val in enumerate(delete_list):
        #print(val)
        #print("idx",idx)
        del machines[val-idx]
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
            print(str(i+1)+" timeout")



if __name__ == '__main__':
    nbMachines = int(re.findall(r'\d+',sys.argv[1])[0])
    ip_list=[]
    i=10
    while len(ip_list)<nbMachines:
        machine=f"tp-4b01-{i:02d}"
        ssh_result=ssh("hostname",[machine])
        if ssh_result: ##if not empty list
            ip_list.append(ssh_result[0])
        i+=1
    print("Number of machines:",len(ip_list))
    """    
    for i in range(10,nbMachines + 1):
            machine=f"tp-4b01-{i:02d}"
            ip_list.append(machine)
    ip_list=ssh("hostname",ip_list) #keep only working machines
    """
    with open("list_ip.txt", "w") as f:
        for index,machine in enumerate(ip_list):
            #print(len(ip_list))
            if index < len(ip_list)-1:
                f.write(machine+"\n")
            else:
                f.write(machine)

    ip_list=ssh("mkdir -p /tmp/pmecchia-20",ip_list)
    scp("Slave.py", "/tmp/pmecchia-20",ip_list)
    scp("list_ip.txt", "/tmp/pmecchia-20/machines.txt", ip_list)

