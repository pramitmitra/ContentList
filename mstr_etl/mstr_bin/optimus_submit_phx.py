import json
import shlex
import sys
import subprocess
import getopt
import time

def curl(url):
    args = shlex.split(url)
    process = subprocess.Popen(args, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    exit_code = process.returncode
    if (exit_code !=0):
        print 'curl fail with non-zero exit code'
        print (exit_code, stdout, stderr)
        return None
    else:
        js_ret = json.loads(stdout)
        success = js_ret.get('success')
        if success == True:
            result=js_ret.get('result')
            if result is not None:
                return result
        else:
            print 'optimus returned abnormal result'
            print (exit_code, stdout, stderr)
            return None
                
            
def submit(src, dst):
    data = json.dumps({"src": src, "dst": dst})
    submit_url = '''curl -s -k --negotiate -u : -H "Content-Type: application/json" -X POST -d '{0}' https://bdp.vip.ebay.com/optimus-phx/job'''
    submit_cmd = submit_url.format(data)
    ret = curl(submit_cmd)
    if ret is not None:
        return ret['jobId']
    else:
        sys.exit(-1)

def getStatus(jobid):
    query_url = '''curl -s -k --negotiate -u : -H "Content-Type: application/json" -X GET https://bdp.vip.ebay.com/optimus-phx/job/{0}'''
    query_cmd = query_url.format(jobid)
    ret = curl(query_cmd)
    if ret is not None:
        return ret['status']
    else:
        sys.exit(-1)

def usage():
    print 'Usage: optimus_submit.py -s src -d dst -q queue'

def parseArgs(argv):
    src = ''
    dst = ''
    queue = ''
    try:
        opts, args = getopt.getopt(argv,"hs:d:q:",["src=","dst=","queue="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-s", "--src"):
            src = arg
        elif opt in ("-d", "--dst"):
            dst = arg
        elif opt in ("-q", "--queue"):
            queue = arg
    if (src == '') or (dst == ''):
        usage()
        sys.exit(2)
    else:
        return src, dst
    
def main(argv):
    src, dst = parseArgs(argv)
    jobid = submit(src, dst)
    finished = False
    while (not finished):
        status = getStatus(jobid)
        if status == 2:
            print "Optimus job successed"
            return 0
        elif status ==3:
            print "Optimus job failed"
            return -1
        else:
            print "Optimus job is running, sleep..."
            time.sleep(20)

if __name__ == "__main__":
    main(sys.argv[1:])
            



