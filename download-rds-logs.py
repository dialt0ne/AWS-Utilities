#!/usr/bin/env python

import logging
import argparse
import os
import boto3
import re
import time
import math

PROGNAME = "download-rds-logs.py"
AWS_REGIONS = [
    "us-east-1", "us-west-1", "us-west-2",
    "ap-south-1", "ap-southeast-1", "ap-southeast-2",
    "ap-northeast-1", "ap-northeast-2",
    "eu-west-1", "eu-central-1",
    "sa-east-1"
]


def create_parser(program_name):
    parser = argparse.ArgumentParser(prog=program_name)
    parser.add_argument(
        "-d", "--debug",
        action="store_true", dest="debug",
        help="Turn on debug logging",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true", dest="quiet",
        help="turn off all logging",
    )
    parser.add_argument(
        "-i", "--instance",
        action="store", dest="instance", required=True,
        help="instance name",
    )
    parser.add_argument(
        "-o", "--output",
        action="store", dest="output_dir",
        default="./",
        help="output directory",
    )
    parser.add_argument(
        "-r", "--region",
        action="store", dest="region",
        default="us-east-1", choices=AWS_REGIONS,
        help="AWS region",
    )
    parser.add_argument(
        "-m", "--match",
        action="store", dest="logfile_match",
        help="Only download logs matching regexp",
    )
    parser.add_argument(
        "-l", "--lines", action="store",
        type=int, dest="lines",
        default=1000,
        help="Initial number of lines to request per chunk. "
        "Number of lines will be reduced if logs get truncated.",
    )
    parser.add_argument(
        "-b", "--backoff",
        action="store", type=int, dest="backoff",
        default=10,
        help="Max times to sleep after exponential backoff due to throttling ",
    )
    return parser


def _main():
    parser = create_parser(PROGNAME)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else (logging.ERROR if args.quiet else logging.INFO))

    if not os.path.exists(args.output_dir):
        os.mkdir(args.output_dir)

    client = boto3.client(
        "rds",
        region_name=args.region,
    )
    sleepcount = 0
    while sleepcount < args.backoff:
        try:
            response = client.describe_db_log_files(
                DBInstanceIdentifier=args.instance
            )
            break
        except:
            sleeptime = math.pow(2, sleepcount)
            logging.warning("sleep #%s (%s seconds) due to failure" % (sleepcount, sleeptime))
            time.sleep(sleeptime)
            sleepcount += 1
            continue
    if sleepcount == args.backoff:
        logging.error("Error describing log files for instance: %s" % (args.instance))
        return
    for log in response['DescribeDBLogFiles']:
        logging.debug(log)
        logfilename = log['LogFileName']
        lines = args.lines

        if args.logfile_match is not None and not re.search(args.logfile_match, logfilename):
            logging.info("Skipping " + logfilename)
            continue

        destination = os.path.join(args.output_dir, os.path.basename(logfilename))

        if os.path.exists(destination):
            statinfo = os.stat(destination)
            if statinfo.st_size == log['Size']:
                logging.info("File %s exists, skipping" % (logfilename))
                continue
            else:
                logging.info("Log file %s exists, but size does not match, redownloading." % (logfilename))
                logging.info("Local files size %d expected size:%d" % (statinfo.st_size, log['Size']))
                os.remove(destination)

        chunk = 0
        with open(destination, "wb") as f:
            more_data = True
            marker = "0"
            sleepcount = 0
            while more_data and sleepcount < args.backoff:
                logging.info("requesting %s marker:%s chunk:%i" % (logfilename, marker, chunk))
                try:
                    result = client.download_db_log_file_portion(
                        DBInstanceIdentifier=args.instance,
                        LogFileName=logfilename,
                        Marker=marker,
                        NumberOfLines=lines,
                    )
                except:
                    sleeptime = math.pow(2, sleepcount)
                    logging.warning("sleep #%s (%s seconds) due to failure" % (sleepcount, sleeptime))
                    time.sleep(sleeptime)
                    sleepcount += 1
                    continue
                sleepcount = 0
                logging.info("AdditionalDataPending:%s Marker:%s" % (str(result['AdditionalDataPending']), result['Marker']))

                if 'LogFileData' in result and result['LogFileData'] is not None:
                    if result['LogFileData'].endswith("[Your log message was truncated]\n"):
                        logging.info("Log segment was truncated")
                        if lines > args.lines * 0.1:
                            lines -= int(10)
                            logging.info("retrying with %i lines" % lines)
                            continue

                    f.write(result['LogFileData'])
                else:
                    logging.error("No LogFileData for file:%s" % (logfilename))

                more_data = 'AdditionalDataPending' in result and result['AdditionalDataPending']
                if 'Marker' in result:
                    marker = result['Marker']
                chunk += 1
                del result['LogFileData']
                logging.debug(result)

            if sleepcount == args.backoff:
                logging.error("Error downloading file:%s - too many errors from AWS " % (logfilename))


if __name__ == "__main__":
    _main()
