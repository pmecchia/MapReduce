import subprocess
import threading
import time
import glob
import os
import re

file = open('list_ip.txt', 'r')
ip_list = file.read().splitlines()


def task(ssh_command,machines,command=None,localPath=None,distantPath=None):
    listproc = []
    timer=100
    login="pmecchia-20"
    for idx,ip in enumerate(machines):
        if ssh_command == "scp":
            if command=="split":
                proc = subprocess.Popen([ssh_command,localPath+str(idx)+".txt",login+"@"+ip+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
            elif command=="result":
                proc = subprocess.Popen([ssh_command,login+"@"+ip+":"+localPath,distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
            else:
                proc = subprocess.Popen([ssh_command,localPath,login+"@"+ip+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        elif ssh_command == "ssh":
            proc = subprocess.Popen([ssh_command,login+"@"+ip,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
            listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            #if code != 0:
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

def multiple_files(input_file,nb_files):

    with open(input_file,'r') as file:
        lines = file.readlines()
    size=len(lines)/nb_files
    for i in range(nb_files):
        with open("./S"+str(i)+".txt",'w') as file:
            for line in lines[int(i*size):int((i+1)*size)]:
                file.write(line)
            file.close()

def result(machines):
    reduces_lines=[]
    t_group = threading.Thread(target=task,args=["scp",machines,"result","/tmp/pmecchia-20/reduces/*.txt","./reduces/"])
    t_group.start()
    t_group.join()
    time.sleep(1)
    [reduces_lines.append(open(f,'r').read()+'\n') for f in glob.glob("./reduces/*.txt")] #lines of all reduce files
    reduces_lines="".join(reduces_lines)
    result_file = open("./result.txt", "a")
    result_file.write(reduces_lines[:-1]) # avoid writing last "\n"
    result_file.close()



def threads():

    threads_dir=[]
    t_dir0 = threading.Thread(target=task,args=["ssh",ip_list,"mkdir -p /tmp/pmecchia-20/splits",None,None])
    t_dir1 = threading.Thread(target=task,args=["ssh",ip_list,"mkdir -p /tmp/pmecchia-20/maps",None,None])
    t_split0 = threading.Thread(target=task,args=["scp",ip_list,"split","./S","/tmp/pmecchia-20/splits"])
    t_split1 = threading.Thread(target=task,args=["ssh",ip_list,"python3 /tmp/pmecchia-20/Slave.py 0 /tmp/pmecchia-20/splits/S*.txt",None,None])

    t_machines = threading.Thread(target=task,args=["scp",ip_list,None,"./list_ip.txt","/tmp/pmecchia-20/machines.txt"])

    t_dir2 = threading.Thread(target=task,args=["ssh",ip_list,"mkdir -p /tmp/pmecchia-20/shuffles",None,None])
    t_dir4 = threading.Thread(target=task,args=["ssh",ip_list,"mkdir -p /tmp/pmecchia-20/shufflesreceived",None,None])

    t_shuffle = threading.Thread(target=task,args=["ssh",ip_list,"python3 /tmp/pmecchia-20/Slave.py 1 /tmp/pmecchia-20/maps/UM*.txt",None,None])

    t_dir3 = threading.Thread(target=task,args=["ssh",ip_list,"mkdir -p /tmp/pmecchia-20/reduces",None,None])

    t_reduce = threading.Thread(target=task,args=["ssh",ip_list,"python3 /tmp/pmecchia-20/Slave.py 2" ,None,None])
    """
    threads_dir.extend([t_dir0,t_dir1,t_dir2,t_dir3,t_dir4,t_machines])
    for idx,i in enumerate(threads_dir):
        i.start()
        print(idx)
    """
    t_dir0.start()
    t_dir1.start()
    t_dir2.start()
    t_dir3.start()
    t_dir4.start()
    t_machines.start()

    t_dir0.join()
    t_dir1.join()
    t_dir2.join()
    t_dir3.join()
    t_dir4.join()
    t_machines.join()

    print("MKDIR FINISHED")


    t_split0.start()
    t_split0.join()

    print("SPLIT FINISHED")
    time.sleep(1)
    start=time.process_time()
    t_split1.start()
    t_split1.join()
    finish=time.process_time()-start
    print("MAP FINISHED in:",finish)
    start=time.process_time()
    t_shuffle.start()
    t_shuffle.join()
    finish=time.process_time()-start
    print("SHUFFLE FINISHED in:",finish)
    start=time.process_time()
    t_reduce.start()
    t_reduce.join()
    finish=time.process_time()-start
    print("REDUCE FINISHED in:",finish)


def hashing(string,hostname):
    #hash_code_list=[]
    encode_string=string.encode('utf-8')
    hash_code = str(int.from_bytes(hashlib.sha256(encode_string).digest()[:4], 'little'))
    #hash_code_list.append(str(hash_code))
    output_file="./"+str(hash_code)+"-"+str(hostname)+".txt"
    f = open(output_file, "a")
    f.write(string)
    f.close()
    return hash_code
import hashlib


if __name__ == '__main__':
    dir_exist=os.path.exists('./reduces')
    if not dir_exist:
        os.mkdir('reduces') #create reduces directory
    #multiple_files("exemple.txt",len(ip_list))
    multiple_files("deontologie_police_nationale.txt",len(ip_list))
    threads()

    result(ip_list)


    #UTLISER MULTIPROCESSING
