import subprocess
import os

file = open('list_ip.txt', 'r')
ip_list = file.read().splitlines()

def ssh(command,machines):
    listproc = []
    timer=5
    login="pmecchia-20"

    for ip in machines:
        proc = subprocess.Popen(["ssh",login+"@"+ip,command],stdin=subprocess.PIPE, stdout = subprocess.PIPE,stderr = subprocess.PIPE)
        listproc.append(proc)

    for i in range(len(listproc)):
        try:
            out, err = listproc[i].communicate(timeout=timer)
            code = listproc[i].returncode
            print(str(i)+" out: '{}'".format(out))
            print(str(i)+" err: '{}'".format(err))
            print(str(i)+" exit: {}".format(code))
        except subprocess.TimeoutExpired:
            listproc[i].kill()
            print(str(i)+" timeout")
            del ip_list[i]

    return ip_list



ip_list=ssh("rm -rf /tmp/pmecchia-20/*",ip_list)
ssh("ls /tmp/pmecchia-20",ip_list)
os.system("rm -rf reduces/*")
os.remove("result.txt")


#ssh("hostname")
