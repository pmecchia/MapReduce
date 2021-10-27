#!/usr/bin/env python

import sys
import os
import hashlib
import socket
import subprocess
import glob
import time
import re

def task(ssh_command,machines,command=None,localPath=None,distantPath=None):
    listproc = []
    timer=100
    login="pmecchia-20"


    for ip in machines:
        if ssh_command == "scp":
            proc = subprocess.Popen([ssh_command,localPath,login+"@"+ip+":"+distantPath],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        elif ssh_command == "ssh":
            proc = subprocess.Popen([ssh_command,login+"@"+ip,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            if code != 0:
                print(str(i)+" out: '{}'".format(out))
                print(str(i)+" err: '{}'".format(err))
                print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")

def hashing(input_file,ip_list):
    hash_dic={}
    file=open(input_file,'r').readlines()
    for line in file: # reading each line
        encode_string=line.encode('utf-8')
        hash_code = str(int.from_bytes(hashlib.sha256(encode_string).digest()[:4], 'little'))
        receiver=ip_list[int(hash_code)%len(ip_list)]
        if receiver in hash_dic:
          hash_dic[receiver].append(line)
        else:
          hash_dic[receiver] = [line]
    return hash_dic


file = open('/tmp/pmecchia-20/machines.txt','r')
ip_list = file.read().splitlines()
file.close()

input_file=sys.argv[2] if len(sys.argv)>2 else None
hostname=socket.gethostname()

################################################################################
#####                   MAP                                               ######
################################################################################

if int(sys.argv[1])==0:
    new_file=[]
    input_file = [f for f in glob.glob("/tmp/pmecchia-20/splits/*.txt")][0]
    number=re.search(r"[0-9]+",input_file.split("/")[-1]).group()
    output_file="/tmp/pmecchia-20/maps/UM"+str(number)+".txt"

    f = open(output_file, "a")
    file_words=open(input_file,'r').read().split()
    file_words=[word+" 1\n" for word in file_words]
    f.write(''.join(file_words))
    f.close()

################################################################################
#####                   SHUFFLE                                           ######
################################################################################



elif int(sys.argv[1])==1:

    hash_dic=hashing(input_file,ip_list)
    #print(hash_dic)
    for key, value in hash_dic.items():
        output_file="/tmp/pmecchia-20/shuffles/to_"+str(key)+"_from_"+str(hostname)+".txt"
        f = open(output_file, "a")
        f.write(''.join(value))
        f.close()
        task("scp",localPath=output_file,distantPath="/tmp/pmecchia-20/shufflesreceived/",machines=[str(key)])



################################################################################
#####                   REDUCE                                            ######
################################################################################

elif int(sys.argv[1])==2:
    reduces_dic={}
    shuffle_files = [f for f in glob.glob("/tmp/pmecchia-20/shufflesreceived/*.txt")]

    for i in shuffle_files:
        lines=open(i,'r').readlines()
        for line in lines:
            word=str(line.split()[0])
            if word in reduces_dic:
              reduces_dic[word]=reduces_dic.get(word)+1
            else:
              reduces_dic[word] = 1
    reduce_file=open("/tmp/pmecchia-20/reduces/"+hostname+".txt","a")

    for key,value in reduces_dic.items():
        reduce_file.write(key+" "+str(value)+"\n")
    reduce_file.close()
