import sys
import argparse
from pyspark.sql import SparkSession
import threading
import traceback
from setu import Setu
import subprocess
import os

if __name__ == "__main__":

    command = ' '.join(sys.argv)
    print("Command used to run this script: ", command)

    args = Setu.parse_args()    

    print("Entered Commandline Arguments: ", args)

    setu = Setu(config_file=args.config, source_mode=args.mode)

    spark = SparkSession \
                .builder \
                .appName(setu.config.appname) \
                .getOrCreate()
    
    try:

        if args.stage not in list(Setu.get_stage_component_mapping().keys()):
            raise ValueError(f"`Setu` doesn't contain `{args.stage}` stage/component. Contained stages & component: {list(Setu.get_stage_component_mapping())}")
        else:
            print(f"Setu: running `{args.stage}` stage/component")

        args = vars(args)
        config_file = args.pop("config")
        stage = args.pop("stage")
        mode = args.pop("mode")

        setu.run(spark, stage, **args)

    except Exception as e:

        print("Encountered an Error: ", e)
        traceback.print_exc()
        if not spark.sparkContext._jsc.sc().isStopped():
            spark.stop()
        raise Exception("Job Failed with above mentioned exception")
        

