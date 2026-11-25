from argparse import ArgumentParser
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_spark_session():
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("omop-standardizer")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    return spark

def parse_yaml(yaml_filepath):
    """
    Parse yaml file with wokflow details
    """
    with open(yaml_filepath, 'r') as file:
        loaded_data = yaml.safe_load(file)
    return loaded_data

def check_yaml(yaml_data):
    """
    Ensure yaml is correctly formatted
    """
    if "id" not in yaml_data:
            raise Exception("YAML file missing 'id': block")
    if "paths" not in yaml_data:
        raise Exception("YAML file missing 'paths:' block")
    if "values" not in yaml_data:
        raise Exception("YAML file missing 'values:' block")

def _convert_rel_path(yaml_filepath, rel_path):
    """
    Convert relative path to absolute path, given the path to yaml it was read from (rel to runner!)
    """
    yaml_pa = Path(yaml_filepath).resolve().parent
    abs_path = (yaml_pa / rel_path).resolve()
    return str(abs_path)

def read_measurements(spark, yaml_filepath, yaml_data):
    """
    Read measurements table
    """
    mpath_rel = yaml_data["paths"]["measurement"]
    mdata = spark.read.csv(_convert_rel_path(yaml_filepath, mpath_rel), header=True, inferSchema=True)
    return mdata

def make_joins(msmt, yaml_filepath, yaml_data):
    """
    Apply user-defined joins to measurements table
    """
    if "joins" not in yaml_data:
        return msmt
    joins = yaml_data["joins"]
    for join in joins.values():
        table_rel = join["path"]
        table = spark.read.csv(_convert_rel_path(yaml_filepath, table_rel), header=True, inferSchema=True)
        if "keys" in join:
            table = table.select(join["keys"])
        if join["cardinality"] == "many":
            collect = [c for c in join["keys"] if c != join["meta_on"]]
            table = table.groupBy(join["meta_on"]).agg(*[F.collect_list(c).alias(c) for c in collect])
        msmt = msmt.join(
            table,
            F.col(join["msmt_on"]) == F.col(join["meta_on"]),
            "left"
        )

    return msmt

def extract_values(msmt, yaml_data):
    """
    Extract text/numeric values and units in measurements acc. user-defined rules
    """
    nval_colexpr = yaml_data["values"]["numeric_value"]
    tval_colexpr = yaml_data["values"]["text_value"]
    unit_colexpr = yaml_data["values"]["unit"]
    msmt = msmt.withColumn("numeric_value", eval(nval_colexpr))
    msmt = msmt.withColumn("text_value", eval(tval_colexpr))
    msmt = msmt.withColumn("unit", eval(unit_colexpr))
    return msmt

def apply_groupings(msmt, yaml_data):
    """
    Apply user-defined groupings
    """
    if "groupings" not in yaml_data:
        return msmt.withColumn("concept_id_std", F.col(yaml_data["id"]))
    return msmt.withColumn("concept_id_std", F.col(yaml_data["id"]))


def harmonize_values_units():
    """
    Harmonize values and units within each group
    """
    pass

def standardize_text_values():
    """
    Standardize text values dataset-wide
    """
    pass

if __name__ == "__main__":
    parser = ArgumentParser(
        prog="omop-standardizer",
        description="standardizes clinical measurements and observations with heterogenous formats"
    )
    parser.add_argument("--file", required=True)
    args = parser.parse_args()
    yaml_filepath = args.file
    yaml_data = parse_yaml(yaml_filepath)
    check_yaml(yaml_data)
    spark = get_spark_session()
    msmt = read_measurements(spark, yaml_filepath, yaml_data)
    msmt.limit(5).show()