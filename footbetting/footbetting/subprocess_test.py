import subprocess
import shlex
import re

command_str = "proxybroker find --types HTTP HTTPS --lvl High --countries CN --strict -l 10 --outfile ./proxies.txt"
command_list = shlex.split(command_str)
p = subprocess.Popen(command_list)
p.wait()
proxy_list = []
for line in open("./proxies.txt","r"):
    #print line

    m = re.match(r"<Proxy CN (\d+\.?\d*)s .* (\d+.\d+.\d+.\d+):(\d+)>", line)

    # proxy_list
    proxy_list.append(m.groups())
proxy_list.sort(key=lambda x:x[0])
print(proxy_list[0])
    # print type(m.groups())
    # print len(m.groups())
    # print m.groups()
