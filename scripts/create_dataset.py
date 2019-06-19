#!/usr/bin/env python2

import os
import argparse
import json


def make_new_dataset(dataset_name, dataset_description, input_tables_directory, input_assets_directory, output_name):
    """
    Adds JSON files for assets and tables to a dataset, generates relationships and then writes the final JSON to disk.
    :param dataset_name: the schema name for the dataset
    :param dataset_description: description of the schema
    :param input_tables_directory: the directory containing tables JSON files
    :param input_assets_directory: the directory containing assets JSON files
    :param output_name: the name of the output JSON, containing the combined assets, relationships and tables JSONs
    """
    dataset = init_dataset(dataset_name=dataset_name,
                           dataset_description=dataset_description)

    input_tables = get_json(parent_directory=input_tables_directory)

    dataset_with_tables_and_relationships = create_and_add_tables_and_relationships(input_tables=input_tables,
                                                                                    dataset=dataset)

    input_assets = get_json(parent_directory=input_assets_directory)

    dataset_with_tables_assets_and_relationships = create_and_add_assets(input_assets=input_assets,
                                                                         dataset=dataset_with_tables_and_relationships)

    save_dataset_as_json(dataset=dataset_with_tables_assets_and_relationships,
                         output_name=output_name)


def init_dataset(dataset_name, dataset_description):
    """
    Creates any empty dataset as a dict that has a study name, description, and schema.
    :param dataset_name: the schema name for the dataset
    :param dataset_description: description of the schema
    """
    dataset = {
        "name": dataset_name,
        "description": dataset_description,
        "schema": {
            "tables": [],
            "relationships": [],
            "assets": []
        }
    }

    return dataset


def get_json(parent_directory):
    """
    Adds JSON files to a dict with each key being the name field of a json file.
    and the value being the corresponding json.
    :param parent_directory: the directory containing JSON files to add to the dict
    """
    input_dict = {}
    for file in os.listdir(parent_directory):
        if file.endswith(".json"):
            with open(os.path.join(parent_directory, file), "r") as json_file:
                json_as_dict = json.load(json_file)
            # add the name field field of the json as a kay to make each json searchable
            input_dict.update({json_as_dict["name"]: json_as_dict})

    return input_dict


def create_and_add_tables_and_relationships(input_tables, dataset):
    """
    Creates tables and relationships and then adds them to the dataset's relationship and asset fields.
    :param input_tables: the input json tables that have been added to a dict
    :param dataset: the created dataset that will be written to disk
    """
    for input_table_name in input_tables:
        table, relationships = create_table_and_relationships(input_table=input_tables[input_table_name],
                                                              input_tables=input_tables)
        dataset["schema"]["tables"].append(table)

        # only add a relationship the schema if any of the column from the given input table contains reference(s)
        if len(relationships) > 0:
            dataset["schema"]["relationships"].extend(relationships)

    return dataset


def create_and_add_assets(input_assets, dataset):
    """
    For every input asset it creates an asset by adding all the tables and relationship names to the asset and
    then adds each created assets to the dataset.
    :param input_assets: the input json assets that have been added to a dict
    :param dataset: the created dataset that will be written to disk
    """
    for input_asset_name in input_assets:
        asset = create_asset(input_asset=input_assets[input_asset_name],
                             dataset=dataset)
        dataset["schema"]["assets"].append(asset)

    return dataset


def create_asset(input_asset, dataset):
    """
    Creates an asset by adding all the tables and relationship names to the asset.
    :param input_asset: the input json asset that has been added to a dict
    :param dataset: the created dataset that will be written to disk
    """
    asset = init_asset(asset_name=input_asset["name"],
                       root_table=input_asset["rootTable"],
                       root_column=input_asset["rootColumn"])

    # add tables and the follow section to the asset
    asset_with_tables = add_asset_tables(asset=asset, dataset=dataset)
    asset_with_tables_and_follow = add_asset_follow(asset=asset_with_tables, dataset=dataset)

    return asset_with_tables_and_follow


def add_asset_tables(asset, dataset):
    """
    Adds all the table names from the dataset to the asset.
    :param asset: the asset to be created and added to the dataset with table names
    :param dataset: the created dataset that will be written to disk
    """
    for table in dataset["schema"]["tables"]:
        asset["tables"].append(
            {
                "name": table["name"],
                "columns": []
            }
        )
    return asset


def add_asset_follow(asset, dataset):
    """
    Adds all the table names from the dataset to the asset.
    :param asset: the asset to be created and added to the dataset with relationship names
    :param dataset: the created dataset that will be written to disk
    """
    for relationship in dataset["schema"]["relationships"]:
        asset["follow"].append(relationship["name"])

    return asset


def init_asset(asset_name, root_table, root_column):
    """
    Adds all the table names from the dataset to the asset.
    :param asset_name: the name of the asset being  created and added to the dataset
    :param root_table: the name of the root table for the asset
    :param root_column: the name of the root column for the asset
    """
    asset = {
        "name": asset_name,
        "rootTable": root_table,
        "rootColumn": root_column,
        "tables": [],
        "follow": []
    }
    return asset


def create_table_and_relationships(input_table, input_tables):
    """
    Creates a table and relationships using the input tables fields and reference field.
    :param input_table: the input table to be used to create a new table and relationships
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    table_name = input_table["name"]
    table = init_table(table_name=table_name)
    relationships = []
    for input_column in input_table["columns"]:
        column, new_relationships = create_column_and_relationships(input_column=input_column,
                                                                    table_name=table_name,
                                                                    input_tables=input_tables)
        table["columns"].append(column)

        # only add a relationship to the table if the given input column contains reference(s)
        if len(new_relationships) > 0:
            relationships.extend(new_relationships)

    return table, relationships


def init_table(table_name):
    """
    Creates a new table as dict with a table name and an empty list for the columns field.
    :param table_name: the name of input table to be used to create a new table
    """
    table = {
        "name": table_name,
        "columns": []
    }

    return table


def create_column_and_relationships(input_column, table_name, input_tables):
    """
    Creates a column and relationships, keeping ony the name, data type and column field from the input column.
    :param input_column: the input column used to create the new column and relationships
    :param table_name: the name of input table that has the input_column
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    relationships = []
    # only add relationships if the input column has them
    if "references" in input_column.keys():
        relationships = create_relationships(references=input_column["references"],
                                             from_table_name=table_name,
                                             from_column_name=input_column["name"],
                                             array_of=input_column.get("array_of"),
                                             input_tables=input_tables)

    column = init_column(column_name=input_column["name"],
                         data_type=input_column["datatype"],
                         array_of=input_column.get("array_of"))

    return column, relationships


def create_relationships(references, from_table_name, from_column_name, array_of, input_tables):
    """
    Creates relationships using the reference field from the input column.
    :param references: the input reference to generate relationships
    :param from_table_name: the name of input table that has the input_column
    :param from_column_name: the name of the input_column
    :param array_of: the value of the array_of of input column indicating if the column should be an array
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    relationships = []
    for reference in references:
        relationship = create_relationship(reference=reference,
                                           from_table_name=from_table_name,
                                           from_column_name=from_column_name,
                                           array_of=array_of,
                                           input_tables=input_tables)
        relationships.append(relationship)

    return relationships


def create_relationship(reference, from_table_name, from_column_name, array_of, input_tables):
    """
    Validates and creates a relationship using input table and a reference from reference field from the input column.
    :param reference: the input reference to generate a relationship
    :param from_table_name: the name of input table that has the input_column
    :param from_column_name: the name of the input_column
    :param array_of: the value of the array_of of input column indicating if the column should be an array
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    to_table_name = reference["table_name"]
    to_column_name = reference["column_name"]

    # check the the from and to column and tables exist in the input tables
    validate_relationship(from_table_name=from_table_name,
                          from_column_name=from_column_name,
                          to_table_name=to_table_name,
                          to_column_name=to_column_name,
                          input_tables=input_tables)

    # if the relationship is valid
    relationship = init_relationship(from_table_name=from_table_name,
                                     from_column_name=from_column_name,
                                     to_table_name=to_table_name,
                                     to_column_name=to_column_name,
                                     array_of=array_of)

    return relationship


def validate_relationship(from_table_name, from_column_name, to_table_name, to_column_name, input_tables):
    """
    Validates a relationship by checking that the table/column names in a relation exits in the input tables
    :param from_table_name: the name of input table containing the input_column
    :param from_column_name: the name of the input_column
    :param to_table_name: the name of table the input column relates to
    :param to_column_name: the name of column the input column relates to
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    # validate the from table/column's exists in the input tables
    validate_table_name_and_column_name(table_name=from_table_name,
                                        column_name=from_column_name,
                                        input_tables=input_tables)

    # validate the to table/column's exists in the input tables
    validate_table_name_and_column_name(table_name=to_table_name,
                                        column_name=to_column_name,
                                        input_tables=input_tables)


def validate_table_name_and_column_name(table_name, column_name, input_tables):
    """
    Validates a relationship checking the the given table and column exist in input tables.
    :param table_name: the name of input table containing has the input_column
    :param column_name: the name of the input_column
    :param input_tables: the input tables that have been added to a dict and used to validate the created relationships
    """
    input_columns = input_tables.get(table_name).get("columns")
    if input_columns:
        # columns is a list of dicts/columns containing multiple fields
        for column in input_columns:
            if column["name"] == column_name:
                return

    # raise in error if either the column or table name does not exist in the column names
    raise SystemExit("Error validating table name and relationship: "
                     "Column, {}, in table, {} does not exist"
                     .format(column_name, table_name) + "\n")


def init_relationship(from_table_name, from_column_name, to_table_name, to_column_name, array_of):
    """
    Creates a relationship given the input table/column name and the corresponding table/column name they relate to.
    :param from_table_name: the name of input table containing the input_column
    :param from_column_name: the name of the input_column
    :param to_table_name: the name of table the input column relates to
    :param to_column_name: the name of column the input column relates to
    :param array_of: the value of the array_of of input column indicating if the column should be an array
    """
    relationship_name = "{}_{}_to_{}_{}".format(from_table_name, from_column_name, to_table_name, to_column_name)
    cardinality = "one"

    # if the column should be an arry of links the cardinality is one to many
    if array_of is True:
        cardinality = "many"
    relationship = {
        "name": relationship_name,
        "from": {
            "table": from_table_name,
            "column": from_column_name,
            "cardinality": "one"
        },
        "to": {
            "table": to_table_name,
            "column": to_column_name,
            "cardinality": cardinality}
    }

    return relationship


def init_column(column_name, data_type, array_of):
    """
    Creates a column given the input column name, data_type and if it is an array t
    :param column_name: the name of the input_column
    :param data_type: the primitive type the value(s) of the column should be
    :param array_of: the value of the array_of of input column indicating if the column should be an array
    """
    # if the data type is a link (references another table) it should really be a string
    if data_type == "link":
        data_type = "String"

    column = {
        "name": column_name,
        "datatype": data_type
    }

    # if array of is false still add the field
    if array_of != None:
        column["array_of"] = array_of
    return column


def save_dataset_as_json(dataset, output_name):
    """
    Writes the combined JSON of assets, tables and relationships to disk.
    :param dataset: the combined dataset to be written to disk
    :param output_name: the name of the output JSON, containing the combined assets, relationships and tables JSONs
    """
    with open(output_name, "w") as output_file:
        json.dump(dataset,
                  output_file,
                  sort_keys=False,
                  indent=2)


if __name__ == "__main__":
    # get the argument inputs
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-name",
                        "-n",
                        dest="dataset_name",
                        required=True,
                        help="the schema name for the dataset")
    parser.add_argument("--dataset-description",
                        "-d",
                        dest="dataset_description",
                        required=True,
                        help="description of the schema")
    parser.add_argument("--input-tables-directory",
                        "-i",
                        dest="input_tables_directory",
                        required=True,
                        help="the directory containing tables JSON files")
    parser.add_argument("--input-assets-directory",
                        "-i",
                        dest="input_assets_directory",
                        required=True,
                        help="the directory containing assets JSON file(s)")
    parser.add_argument("--output-name",
                        "-o",
                        dest="output_name",
                        required=True,
                        help="the name of the output JSON, containing the combined assets, relationships and tables")
    args = parser.parse_args()

    make_new_dataset(dataset_name=args.dataset_name,
                     dataset_description=args.dataset_description,
                     input_tables_directory=args.input_directory,
                     input_assets_directory=args.input_assets_directory,
                     output_name=args.output_name)
