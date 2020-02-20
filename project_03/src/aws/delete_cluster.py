#!/usr/bin/python3

import sys
import configparser
import boto3
import time

import cluster_status


def main(argv):
    # Get configuration data
    config = configparser.ConfigParser()
    config.read_file(open('../config/dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    redshift = boto3.client('redshift',
                            region_name="us-east-1",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    print("Prior to deletion status: ")
    cluster_status.main()
    # WARNING: permanently deletes Redshift cluster.
    # Delete the cluster
    redshift.delete_cluster(
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

    # Wait for deletion status to update before printing status.
    time.sleep(1.5)

    print("After deletion status: ")
    cluster_status.main()


if __name__ == "__main__":
    main(sys.argv[:])
