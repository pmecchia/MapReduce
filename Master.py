import subprocess
import threading
import time
import glob
import os
import codecs
import re

file = open('INF727_Systemes_repartis/list_ip.txt', 'r')
ip_list = file.read().splitlines()


def task(ssh_command,machines,command=None,localPath=None,distantPath=None):
    listproc = []
    #timer=200
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
            #timeout=timer
            out, err = listproc[i].communicate()
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

def multiple_files(input_file,nb_files):

    with open(input_file,'r',encoding='utf-8') as file:
        lines = file.readlines()
    size=len(lines)/nb_files
    for i in range(nb_files):
        with open("INF727_Systemes_repartis/input_splits/S"+str(i)+".txt",'w',encoding='utf-8') as file:
            for line in lines[int(i*size):int((i+1)*size)]:
                file.write(line)
            file.close()

def result(machines):
    task("scp",machines,"result","/tmp/pmecchia-20/reduces/*.txt","INF727_Systemes_repartis/reduces/")
    final_dict = {}
    for reduce in glob.glob("INF727_Systemes_repartis/reduces/*.txt"):
        file = open(reduce, "r", encoding='utf-8')
        contents = file.read()
        dictionary = eval(contents)
        file.close()
        final_dict.update(dictionary)
    final_dict = dict(sorted(final_dict.items(), key=lambda item: item[1], reverse=True))  # sort final dict
    with open("INF727_Systemes_repartis/result.txt", "a") as f:
        print(final_dict, file=f)

def final_dict():
    final_dict={}
    for reduce in glob.glob("INF727_Systemes_repartis/reduces/*.txt"):
        file =open(reduce, "r",encoding='utf-8')
        contents = file.read()
        dictionary = eval(contents)
        file.close()
        final_dict.update(dictionary)
    final_dict=dict(sorted(final_dict.items(), key=lambda item: item[1],reverse=True)) #sort final dict
    with open("INF727_Systemes_repartis/result.txt", "a") as f:
        print(final_dict, file=f)



if __name__ == '__main__':
    dir_reduces_exist=os.path.exists('INF727_Systemes_repartis/reduces')
    dir_split_exist = os.path.exists('INF727_Systemes_repartis/input_splits')
    if not dir_reduces_exist:
        os.mkdir('INF727_Systemes_repartis/reduces') #create reduces directory
    if not dir_split_exist:
        os.mkdir('INF727_Systemes_repartis/input_splits')

    start1=time.time()
    multiple_files("INF727_Systemes_repartis/amazon_file-1.txt", len(ip_list))
    finish = time.time() - start1
    print('SPLIT INPUT COMPLETED IN: ' + str(finish))

    start = time.time()
    task("ssh",ip_list,"mkdir -p /tmp/pmecchia-20/splits",None,None)
    task("ssh",ip_list,"mkdir -p /tmp/pmecchia-20/maps",None,None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/shuffles", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/shufflesreceived", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/reduces", None, None)
    finish = time.time() - start
    print('ALL MKDIR COMPLETED IN: ' + str(finish))

    start = time.time()
    task("scp", ip_list, None, "INF727_Systemes_repartis/list_ip.txt", "/tmp/pmecchia-20/machines.txt")
    finish = time.time() - start
    print('MACHINE LIST SENT IN: ' + str(finish))

    start = time.time()
    task("scp",ip_list,"split","INF727_Systemes_repartis/input_splits/S","/tmp/pmecchia-20/splits")
    finish = time.time() - start
    print('SPLIT COMPLETED IN: ' + str(finish))

    start = time.time()
    task("ssh", ip_list, "python3 /tmp/pmecchia-20/Slave.py 0 /tmp/pmecchia-20/splits/S*.txt", None, None)
    finish = time.time() - start
    print('MAP COMPLETED IN: ' + str(finish))

    start = time.time()
    task("ssh",ip_list,"python3 /tmp/pmecchia-20/Slave.py 1 /tmp/pmecchia-20/maps/UM*.txt",None,None)
    finish = time.time() - start
    print('SHUFFLE COMPLETED IN: ' + str(finish))

    start = time.time()
    task("ssh",ip_list,"python3 /tmp/pmecchia-20/Slave.py 2" ,None,None)
    finish = time.time() - start
    print('REDUCE COMPLETED IN: ' + str(finish))

    start = time.time()
    result(ip_list)
    finish = time.time() - start
    print('RESULT IN: ' + str(finish))

    finish = time.time() - start1
    print('TOTAL COMPLETED IN: ' + str(finish))