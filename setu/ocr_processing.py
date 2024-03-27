import argparse
from base import SetuComponent,SetuStage
from pyspark.sql import SparkSession

class OCRProcessingComponent(SetuComponent):
    def __init__(self, config: argparse.Namespace) -> None:
        super().__init__(config)
        pass

    @classmethod
    def add_cmdline_args(parser: argparse.ArgumentParser,full_pipeline=False) -> None:
        if full_pipeline:
            pass
        else:
            pass
    
    @classmethod
    def get_stage_mapping(cls) -> dict:
        pass

    def run_stage(self, spark:SparkSession, stage:str, **kwargs):
        return self.stages[stage].run(spark, **kwargs)

    def run(self, spark:SparkSession, stage:str, **kwargs):
        if stage == self.name:
            raise NotImplementedError(f"`run` function for running the entire component has not been implemented for class `{self.name}`")
        else:
            return self.run_stage(spark=spark, stage=stage, **kwargs)