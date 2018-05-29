import subprocess
import shlex
import re

def crawl_proxy_ip():
    command_str = "proxybroker find --types HTTP HTTPS --lvl High --countries CN --strict -l 10 --outfile ./footbetting/proxies.txt"
    command_list = shlex.split(command_str)
    p = subprocess.Popen(command_list)
    print("crawl proxy ip ...")
    p.wait()
    print("crawl proxy ip done")
    proxy_list = []
    for line in open("./footbetting/proxies.txt","r"):
        m = re.match(r"<Proxy CN (\d+\.?\d*)s .* (\d+.\d+.\d+.\d+):(\d+)>", line)
        proxy_list.append(m.groups())
    proxy_list.sort(key=lambda x:x[0])
    best_proxy = proxy_list[0]
    print("best_proxy, response time:%s, ip:%s, port:%s"%best_proxy)
    return best_proxy[1],best_proxy[2]