import subprocess
import time
import glob
import os
import sys


def task(ssh_command,machines,command=None,localPath=None,distantPath=None):
    listproc = []
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
        with open("input_splits/S"+str(i)+".txt",'w',encoding='utf-8') as file:
            for line in lines[int(i*size):int((i+1)*size)]:
                file.write(line)
            file.close()

def result(machines):
    start = time.time()
    task("scp",machines,"result","/tmp/pmecchia-20/reduces/*.txt","reduces/")
    finish = time.time() - start
    #print('REDUCE RECEIVED IN: ' + str(finish))
    print(finish)

    start = time.time()
    final_dict = {}
    for reduce in glob.glob("reduces/*.txt"):
        file = open(reduce, "r", encoding='utf-8')
        contents = file.read()
        dictionary = eval(contents)
        file.close()
        final_dict.update(dictionary)
    final_dict = dict(sorted(final_dict.items(), key=lambda item: item[1], reverse=True))  # sort final dict
    with open("result.txt", "a") as f:
        print(final_dict, file=f)
    finish = time.time() - start
    #print('RESULT FILE IN: ' + str(finish))
    print(finish)

def word_count_local(filename):
    file= open(filename, "r", encoding='utf-8')
    words_dict={}
    for line in file:
        words=line.split()
        for word in words:
            #word=word.lower()
            if word not in words_dict:
                words_dict[word]=1
            else:
                words_dict[word]+=1
    file.close()
    final_dict = dict(sorted(words_dict.items(), key=lambda item: item[1], reverse=True))
    with open("result_static.txt", "w") as f:
        print(final_dict, file=f)

def word_count_distributed(filename):
    start1 = time.time()
    multiple_files(filename, len(ip_list))
    finish = time.time() - start1
    #print('SPLIT INPUT COMPLETED IN: ' + str(finish))
    print(finish)
    start = time.time()
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/splits", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/maps", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/shuffles", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/shufflesreceived", None, None)
    task("ssh", ip_list, "mkdir -p /tmp/pmecchia-20/reduces", None, None)
    finish = time.time() - start
    #print('ALL MKDIR COMPLETED IN: ' + str(finish))
    print(finish)

    start = time.time()
    task("scp", ip_list, "split", "input_splits/S", "/tmp/pmecchia-20/splits")
    finish = time.time() - start
    #print('SPLIT COMPLETED IN: ' + str(finish))
    print(finish)

    start = time.time()
    task("ssh", ip_list, "python3 /tmp/pmecchia-20/Slave.py 0 /tmp/pmecchia-20/splits/S*.txt", None, None)
    finish = time.time() - start
    #print('MAP COMPLETED IN: ' + str(finish))
    print(finish)

    start = time.time()
    task("ssh", ip_list, "python3 /tmp/pmecchia-20/Slave.py 1 /tmp/pmecchia-20/maps/UM*.txt", None, None)
    finish = time.time() - start
    #print('SHUFFLE COMPLETED IN: ' + str(finish))
    print(finish)

    start = time.time()
    task("ssh", ip_list, "python3 /tmp/pmecchia-20/Slave.py 2", None, None)
    finish = time.time() - start
    #print('REDUCE COMPLETED IN: ' + str(finish))
    print(finish)
#
    #start = time.time()
    result(ip_list)
    #finish = time.time() - start
    #print('RESULT IN: ' + str(finish))

    check_result()

def check_result():

    ##get static values
    static_dict={}
    distrib_dict={}
    file = open("result_static.txt", "r", encoding='utf-8')
    contents = file.read()
    static_res= eval(contents)
    file.close()
    static_dict.update(static_res)

    #get distributed values
    file = open("result.txt", "r", encoding='utf-8')
    contents = file.read()
    static_res= eval(contents)
    file.close()
    distrib_dict.update(static_res)
    if list(static_dict.values())[:10]==list(distrib_dict.values())[:10]: ## check 10 first values
        print(1)
    else:
        print(0)

if __name__ == '__main__':

    filename = sys.argv[1]
    option = sys.argv[2]

    dir_reduces_exist=os.path.exists('reduces')
    dir_split_exist = os.path.exists('input_splits')
    if not dir_reduces_exist:
        os.mkdir('reduces') #create reduces directory
    if not dir_split_exist:
        os.mkdir('input_splits')


    if option=="--local":
        start = time.time()
        word_count_local(filename)
    elif option=="--distributed":
        file = open('list_ip.txt', 'r')
        ip_list = file.read().splitlines()
        start=time.time()
        word_count_distributed(filename)
    finish = time.time() - start
    #print('TOTAL COMPLETED : ' + str(finish))
    print(finish)
