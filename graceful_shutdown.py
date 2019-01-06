"""

This script is used to do graceful shutdown for Spark streaming applications running on AWS EMR automatically.


-- HOW TO USE?

1. Run it on an AWS EC2 instance.
2. Pepare pem file on the instance, rename it to "pub-for-team.pem". The pem file is used to access your EMR instances.
3. Command:
python graceful_shutdown.py --cluster <cluster name> --app <spark_application_name> --region <aws region> --flag <identifier>
e.g.,
/usr/local/bin/python graceful_shutdown.py --cluster stg-lfs-an --app stg-lfs-an-mfnn6 --region ap-northeast-1 --flag mfnn6


Edit by Bruce on 2017/09/26

"""

import argparse
import json
from yarn_api_client import ResourceManager
import subprocess
from datetime import datetime


def get_active_cluster_by_name(name, region):
    cluster_info = subprocess.check_output(
        ["/usr/local/bin/aws", "emr", "list-clusters", "--active", "--region", region])
    clusters = json.loads(cluster_info)

    for c in clusters['Clusters']:
        if c["Name"] == name and c["Status"]["State"] in ["WAITING", "RUNNING", "STARTING", "BOOTSTRAPPING"]:
            return c["Id"]

    return None


def get_master_domain_by_clouster_id(cluster_id, region):
    cluster_info = subprocess.check_output(
        ["/usr/local/bin/aws", "emr", "describe-cluster", "--cluster-id", cluster_id, "--region", region])
    cluster = json.loads(cluster_info)
    return cluster['Cluster']['MasterPublicDnsName']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster', action='store', required=True,
                        help='input a EMR cluster name')
    parser.add_argument('--app', action='store', required=True,
                        help='input a spark application name')
    parser.add_argument('--region', action='store', required=True,
                        help='input a AWS region name')
    parser.add_argument('--flag', action='store', help='input a flag to identify the app')
    args = parser.parse_args()

    # get conf
    print 'region: %s, \ncluster: %s, \napp: %s' % (args.region, args.cluster, args.app)

    # get cluster id
    cluster_id = get_active_cluster_by_name(args.cluster, args.region)
    if not cluster_id:
        print 'cluster does not exist...'
        return

    print 'cluster id: %s' % cluster_id

    # get all yarn apps
    master_domain = get_master_domain_by_clouster_id(cluster_id, args.region)
    print 'master instance: %s' % master_domain

    yarn = ResourceManager(master_domain)
    apps = yarn.cluster_applications().data["apps"]['app']
    apps = [x for x in apps if x["state"] not in ("FINISHED", "KILLED", "FAILED")]
    print 'active apps: %s' % map(lambda x: x["name"], apps)

    # find driver url
    driver_url = ""
    for x in apps:
        if x["name"] == args.app:
            driver_url = x["amContainerLogs"]
            break
    if not driver_url:
        print 'failed to find driver...'
        return

    driver_url = driver_url[7:].split(":")[0]
    print 'driver: %s' % driver_url

    # find psid of ApplicationMaster
    cmd = 'ssh -i ~/pub-for-team.pem hadoop@' + driver_url + \
          ' "ps -ef|grep ApplicationMaster|grep -v /bin/bash|grep -v color"'
    print '\ncmd: %s' % cmd
    result = subprocess.check_output(cmd, shell=True)
    result = result.split('\n')
    result = [x for x in result if x]
    print '\nps result: %s' % result

    if len(result) > 1:
        if not args.flag:
            print '\ntoo many applications, please specify a flag to identify your application...'
            return
        else:
            result = filter(lambda x: x.find(args.flag) >= 0, result)
            if not result:
                print 'can not find the specified application master, please check your flag...'
                return

    result = result[0].split()[1]
    print '\nPSID FOUND: %s\n' % result

    # try to kill
    cmd = 'ssh -i ~/pub-for-team.pem hadoop@' + driver_url + \
          ' "sudo kill -SIGTERM %s"' % result
    print 'cmd: %s' % cmd
    result = subprocess.check_output(cmd, shell=True)
    if result:
        print result

    print '[%s] finished to shutdown [%s] gracefully, ' \
          'please wait for about 15~30 minutes.' % (datetime.now(), args.app)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print traceback.format_exc()

