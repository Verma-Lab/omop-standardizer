# omop-standardizer
Standardizer for OMOP measurement and observation tables (csv).

# Instructions
1. You point omop-standardizer to a table with measurements
2. You tell omop-standardizer what metadata tables to join and how
3. You tell omop-standardizer how to extract a text value, numeric value, and unit for each measurement
4. You tell omop-standardizer how to group measurements into buckets
5. Omop-standardizer extracts values and units, groups measurements into buckets, and homogenizes units and values within each bucket

# Usage
- Define a standardize.yaml file with id groups, target units.
- Run standardize.py --file <file_name>

# Syntax
Omop-standardizer uses YAML to define the standardization workflow: in particular, the data file paths, the value/unit extraction logic, and the measurement groups.
The syntax of an omop-standardizer YAML file is as follows:
```YAML

# define relevant file paths for the workflow
paths:
    measurement: <filepath> # relative path to measurements file
    id_to_unit: <filepath> # relative path to id to unit file written after standardization

concept_id: <column name> # name of concept id column to fallback on for ungrouped

# define joins to metadata tables used in defining measurement groups
joins:
    table1:
        path: <filepath> # relative path to metadata table
        keys: ["key1", "key2"] # metadata columns to include (include join key!)
        msmt_on: <measurement_join_key> # msmt col to join on
        meta_on: <metadata_join_key> # metadata col to join on
        cardinality: <one/many> # whether to collect list or not

# define logic for extracting numeric value, unit, and text value
values:
    numeric_value: <pyspark column expression> # pyspark col expr for extracting numeric value
    text_value: <pyspark column expression> # pyspark col expr for extracting text value
    unit: <pyspark column expression> # pyspark col expr for extracting unit assoc. w/ numeric value

groups:
    subgroup1: # e.g. labs/vitals
        bucket1: # e.g. albumin
            id: <integer> # integer id to standardize bucket concept_id to (concept_id_std)
            rule: <pyspark column expression> # pyspark boolean col expr to define bucket membership
            unit: <unit> # name of unit to convert to
```