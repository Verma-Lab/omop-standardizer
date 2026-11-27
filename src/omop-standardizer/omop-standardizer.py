from argparse import ArgumentParser
import yaml
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def get_spark_session():
    print("**************** Creating a new Spark Session ****************")
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
    print("**************** Parsing YAML ****************")
    with open(yaml_filepath, 'r') as file:
        loaded_data = yaml.safe_load(file)
    return loaded_data

def check_yaml(yaml_data):
    """
    Ensure yaml is correctly formatted
    """
    print("**************** Checking YAML ****************")
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
    print("**************** Reading Measurements ****************")
    mpath_rel = yaml_data["paths"]["measurement"]
    mdata = spark.read.csv(_convert_rel_path(yaml_filepath, mpath_rel), header=True, inferSchema=True)
    return mdata

def make_joins(msmt, yaml_filepath, yaml_data):
    """
    Apply user-defined joins to measurements table
    """
    print("**************** Applying Joins ****************")
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
    print("**************** Extracting Values ****************")
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
    print("**************** Applying Groupings ****************")
    if "groups" not in yaml_data:
        return msmt.withColumn("concept_id_std", F.col(yaml_data["id"]))
    return None # TODO: implement

def _read_unit_ladder():
    with open(Path(__file__).parent / "ladder.yaml") as file:
        loaded_data = yaml.safe_load(file)
    return loaded_data

def harmonize_values_units(msmt):
    """
    Harmonize values and ("homogenize") units within each group, defined by std_concept_id
    GUARANTEE:
    1. all rows w/ no numeric value have null std_unit
    2. non-null values of unit_std are homogenous within concept_id_std group (numeric_value_std matches)
    3. a unit is not dropped if it is the selected unit, even if it is not in the conversion ladder
    DOES NOT GUARANTEE:
    1. all conversions succeed: in case of unladdered (unknown), unselected units that must therefore be converted, set numeric_value_std, unit_std = null
    """
    print("**************** Harmonizing Numeric Values ****************")

    # extract rows with numeric values
    has_numeric = msmt.filter(F.col("numeric_value").isNotNull())

    # extract rows without numeric values and nullify units, values
    non_numeric = msmt.filter(F.col("numeric_value").isNull())
    non_numeric = non_numeric.withColumn("numeric_value_std", F.lit(None))
    non_numeric = non_numeric.withColumn("unit_std", F.lit(None))
    
    # determine target unit for each concept id assoc. w/ numeric values
    target_units = has_numeric.groupBy(F.col("concept_id_std")).agg(F.mode("unit").alias("tgt_unit"))

    # TODO: add/override with user-defined units

    # infer target unit for each row
    has_numeric = has_numeric.join(target_units, "concept_id_std", "left")

    # read unit ladder and format for mappings
    unit_ladder = _read_unit_ladder()
    unit_to_ladder = {unit:ladder for ladder, conversions in unit_ladder.items() for unit, base in conversions.items()}
    unit_to_ladder_map = F.create_map([F.lit(x) for kv in unit_to_ladder.items() for x in kv])
    unit_to_base = {unit:base for ladder, conversions in unit_ladder.items() for unit, base in conversions.items()}
    unit_to_base_map = F.create_map([F.lit(x) for kv in unit_to_base.items() for x in kv])

    # infer source unit ladder and target unit ladder
    has_numeric = has_numeric.withColumn("src_ladder", unit_to_ladder_map[F.col("unit")])
    has_numeric = has_numeric.withColumn("tgt_ladder", unit_to_ladder_map[F.col("tgt_unit")])

    # infer source unit base and target unit base
    has_numeric = has_numeric.withColumn("src_base", unit_to_base_map[F.col("unit")])
    has_numeric = has_numeric.withColumn("tgt_base", unit_to_base_map[F.col("tgt_unit")])

    # infer conversion rate as tgt_base/src_base
    has_numeric = has_numeric.withColumn("conversion_rate", F.col("tgt_base") / F.col("src_base"))

    # perform conversion in cases where src and tgt ladders match
    has_numeric = has_numeric.withColumn(
        "numeric_value_std",
        F.when(F.col("unit") == F.col("tgt_unit"), F.col("numeric_value"))
        .when(F.col("src_ladder") == F.col("tgt_ladder"), F.col("numeric_value") * F.col("conversion_rate"))
        .otherwise(None)
    )

    # promote target unit to unit_std in cases where this succeeded
    has_numeric = has_numeric.withColumn(
        "unit_std",
        F.when(F.col("numeric_value_std").isNotNull(), F.col("tgt_unit")).otherwise(None)
    )

    # link numeric values and non-numeric values back together
    has_numeric = has_numeric.drop("tgt_unit", "src_ladder", "tgt_ladder", "src_base", "tgt_base", "conversion_rate")
    res = has_numeric.unionByName(non_numeric)

    return res


def normalize_text_values():
    """
    Standardize text values dataset-wide
    """
    print("**************** Normalizing Textual Values ****************")
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
    msmt = make_joins(msmt, yaml_filepath, yaml_data)
    msmt = extract_values(msmt, yaml_data)
    msmt = apply_groupings(msmt, yaml_data)
    msmt = harmonize_values_units(msmt)
    msmt.limit(5).show()