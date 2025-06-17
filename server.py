import socket
import json
import os
from pymongo import MongoClient
import re
import pymongo
import functools

HOST = 'localhost'
PORT = 65432
CATALOG_FILE = 'catalog.json'

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = None
current_db = None

aggregate_pattern = re.compile(r'(MIN|MAX|AVG|COUNT|SUM)\(([\w*.]+)\)', re.IGNORECASE)


if not os.path.exists(CATALOG_FILE):
    with open(CATALOG_FILE, "w") as f:
        json.dump({"databases": []}, f, indent=4)


def load_catalog():
    with open(CATALOG_FILE, "r") as f:
        return json.load(f)


def save_catalog(data):
    with open(CATALOG_FILE, "w") as f:
        json.dump(data, f, indent=4)


def get_current_database(catalog):
    global current_db
    if not current_db:
        return None
    for db in catalog["databases"]:
        if db["name"] == current_db:
            return db
    return None


def process_command(command):
    global current_db
    tokens = command.strip().split()
    if not tokens:
        return "Error: no command"

    cmd = tokens[0].upper()

    if cmd == "USE":
        if len(tokens) < 2:
            return "Error: no Database name given"
        db_name = tokens[1]
        catalog = load_catalog()
        for db in catalog["databases"]:
            if db["name"] == db_name:
                global current_db, mongo_db
                current_db = db_name
                mongo_db = mongo_client[db_name]
                return f"Using database: {db_name}"
        return "Error: This database doesn't exist"

    elif cmd == "CREATE" and tokens[1].upper() == "DATABASE":
        return create_database(tokens[2])
    elif cmd == "DROP" and tokens[1].upper() == "DATABASE":
        return drop_database(tokens[2])
    elif cmd == "CREATE" and tokens[1].upper() == "TABLE":
        return create_table(tokens[2], tokens[3:])
    elif cmd == "DROP" and tokens[1].upper() == "TABLE":
        return drop_table(tokens[2])
    elif cmd == "INSERT":
        if len(tokens) > 1 and tokens[1].upper() == "BULK":
            return insert_bulk_into_table(tokens)
        else:
            return insert_into_table(tokens)
    elif cmd == "DELETE":
        return delete_from_table(tokens)
    elif cmd == "CREATE" and tokens[1].upper() == "INDEX" or tokens[1].upper() == "UNIQUE":
        return create_index(tokens)
    elif cmd == "SELECT":
        print("SELECT")
        return select_from_table(tokens)
    else:
        return "Unknown command"


def create_database(name):
    catalog = load_catalog()
    if any(db["name"] == name for db in catalog["databases"]):
        return "This database already exists."
    catalog["databases"].append({"name": name, "tables": []})
    save_catalog(catalog)
    return f"Database Created: {name}"


def drop_database(name):
    global current_db
    catalog = load_catalog()
    new_dbs = [db for db in catalog["databases"] if db["name"] != name]
    if len(new_dbs) == len(catalog["databases"]):
        return "This database doesn't exist."
    catalog["databases"] = new_dbs
    if current_db == name:
        current_db = None 
    save_catalog(catalog)
    mongo_client.drop_database(name)
    return f"Database dropped: {name}"


def create_table(name, attributes_raw):
    catalog = load_catalog()
    db = get_current_database(catalog)
    if not db:
        return "Error: There is no selected database. Usage: USE <db_name> command."

    if any(t["name"] == name for t in db["tables"]):
        return "This table already exists."

    attributes = []
    for attr in attributes_raw:
        if ":" not in attr:
            return f"Error: wrong format: {attr}"
        attr_name, attr_type = attr.split(":")
        attributes.append({"name": attr_name, "type": attr_type})

    db["tables"].append({"name": name, "attributes": attributes})
    save_catalog(catalog)

    if mongo_db is not None:
        mongo_db.create_collection(name)

    return f"Table created: {name}"


def drop_table(name):
    catalog = load_catalog()
    db = get_current_database(catalog)
    if not db:
        return "Error: There is no selected database. Usage: USE <db_name> command."

    new_tables = [t for t in db["tables"] if t["name"] != name]
    if len(new_tables) == len(db["tables"]):
        return "Table could not be found."

    db["tables"] = new_tables
    save_catalog(catalog)

    if mongo_db is not None:
        mongo_db[name].drop()

        collections_to_drop = []
        for coll_name in mongo_db.list_collection_names():
            if coll_name.startswith(f"{name}_") and (coll_name.endswith("_index") or coll_name.endswith("_uniqindex")):
                collections_to_drop.append(coll_name)

        for coll_to_drop in collections_to_drop:
            mongo_db[coll_to_drop].drop()

    return f"Table dropped: {name}"

def parse_value_string_to_dict(value_str, table_attributes):
    parsed_obj = {}
    if not value_str: # Kezeljuk az ures stringet
        return parsed_obj
    
    parts = value_str.split("#")
    
    for i, attr in enumerate(table_attributes):
        if i == 0: 
            continue
        
        if (i - 1) < len(parts): 
            val_str = parts[i-1]
            try:
                if attr["type"] == "int":
                    parsed_obj[attr["name"]] = int(val_str)
                elif attr["type"] == "float":
                    parsed_obj[attr["name"]] = float(val_str)
                elif attr["type"] == "str":
                    parsed_obj[attr["name"]] = val_str
                else: 
                    parsed_obj[attr["name"]] = val_str
            except ValueError:
                parsed_obj[attr["name"]] = None 
        else:
            parsed_obj[attr["name"]] = None 
    return parsed_obj

def insert_into_table(tokens):
    if tokens[1].upper() != "INTO" or "VALUES" not in tokens:
        return "Syntax error in INSERT"

    table_name = tokens[2]
    values_index = tokens.index("VALUES")
    values_raw = tokens[values_index + 1:]

    if mongo_db is None:
        return "Error: No database selected"

    catalog = load_catalog()
    db = get_current_database(catalog)
    table = next((t for t in db["tables"] if t["name"] == table_name), None)
    if not table:
        return "Error: Table does not exist"

    attributes = table["attributes"]
    if len(values_raw) != len(attributes):
        return f"Error: Expected {len(attributes)} values, got {len(values_raw)}"
    
    _id_value = None
    value_string_parts = []
    original_document = {}

    for i, (attr, val_str) in enumerate(zip(attributes, values_raw)):
        name = attr["name"]
        typ = attr["type"]
        converted_val = None

        try:
            if typ == "int":
                converted_val = int(val_str)
            elif typ == "float":
                converted_val = float(val_str)
            elif typ == "str":
                converted_val = val_str.strip('"').strip("'")
            else:
                return f"Error: Unsupported type '{typ}' for attribute '{name}'"
        except ValueError:
            return f"Error: Value '{val_str}' cannot be converted to type '{typ}' for attribute '{name}'"
        
        if i == 0:
            _id_value = str(converted_val)
        else:
            value_string_parts.append(str(converted_val))

        original_document[name] = converted_val

    final_value_string = "#".join(value_string_parts)

    collection = mongo_db[table_name]
    if collection.find_one({"_id": _id_value}):
        return f"Error: A record with primary key '{_id_value}' already exists in table '{table_name}'"

    # fk ellenorzes
    for attr in attributes:
        name = attr["name"]
        if name.endswith("_id"):
            ref_table = name[:-3]
            ref_key_val_raw = original_document.get(name)

            ref_key_val = str(ref_key_val_raw)

            if ref_table in mongo_db.list_collection_names():
                ref_coll = mongo_db[ref_table]
                if ref_coll.find_one({"_id": ref_key_val}) is None:
                    return f"Error: Foreign key constraint failed for '{name}' with value '{ref_key_val}' in table '{table_name}'"
                
    collection.insert_one({"_id": _id_value, "value": final_value_string})

    # index kezeles
    for attr in attributes[1:]:
        name = attr["name"]
        value = original_document.get(name)

        if value is None:
            continue

        uniq_index_name = f"{table_name}_{name}_uniqindex"
        if uniq_index_name in mongo_db.list_collection_names():
            uniq_index = mongo_db[uniq_index_name]

            if uniq_index.find_one({"key": value}):
                return f"Error: Unique constraint failed for '{name}' with value '{value}' in table '{table_name}'"
            uniq_index.insert_one({"key": value, "value": _id_value})

        index_name = f"{table_name}_{name}_index"
        if index_name in mongo_db.list_collection_names():
            index_coll = mongo_db[index_name]
            existing = index_coll.find_one({"key": value})
            if existing:
                index_coll.update_one(
                    {"_id": existing["_id"]},
                    {"$push": {"value": _id_value}}
                )
            else:
                index_coll.insert_one({"key": value, "value": [_id_value]})
    
    return f"Row inserted into {table_name} with key {_id_value}"

def delete_from_table(tokens):
    if len(tokens) < 5 or tokens[1].upper() != "FROM" or "WHERE" not in tokens:
        return "Syntax error in DELETE"

    table_name = tokens[2]

    where_clause = tokens[tokens.index("WHERE") + 1:]
    where_str = " ".join(where_clause)
    if "=" not in where_str:
        return "Syntax error in WHERE clause"

    cond_field_raw, cond_value_raw = where_str.split("=", 1)
    cond_field = cond_field_raw.strip()
    cond_value_parsed = cond_value_raw.strip().strip('"').strip("'")

    if mongo_db is None:
        return "Error: No database selected"

    catalog = load_catalog()
    db = get_current_database(catalog)
    if not db:
        return "Error: No database selected in catalog"

    table_info = next((t for t in db["tables"] if t["name"] == table_name), None)
    if not table_info:
        return f"Error: Table '{table_name}' does not exist in the current database."
    attributes = table_info["attributes"]

    collection = mongo_db[table_name]
    keys_to_delete_as_ids = []  # torlendo id-k
    
    # ha _id mezore vonatkozik
    if cond_field == attributes[0]["name"]:
        try:
            pk_type = attributes[0]["type"]
            if pk_type == "int":
                cond_value_for_id_lookup = str(int(cond_value_parsed))
            elif pk_type == "float":
                cond_value_for_id_lookup = str(float(cond_value_parsed))
            else:
                cond_value_for_id_lookup = cond_value_parsed
        except ValueError:
            return f"Error: Type mismatch for primary key condition: expected {pk_type}, got {cond_value_parsed}"
        
        doc = collection.find_one({"_id": cond_value_for_id_lookup})
        if doc:
            keys_to_delete_as_ids.append(doc["_id"])
    else:
        index_collection_name = f"{table_name}_{cond_field}_index"
        uniq_index_collection_name = f"{table_name}_{cond_field}_uniqindex"

        found_in_index = False
        if index_collection_name in mongo_db.list_collection_names():
            index_collection = mongo_db[index_collection_name]
            index_entry = index_collection.find_one({"key": cond_value_parsed})

            if index_entry:
                if isinstance(index_entry["value"], list):  # nem unique index
                    keys_to_delete_as_ids.extend(index_entry["value"])
                else:
                    keys_to_delete_as_ids.append(index_entry["value"])
                found_in_index = True
        elif uniq_index_collection_name in mongo_db.list_collection_names():
            uniq_index_coll = mongo_db[uniq_index_collection_name]
            uniq_index_entry = uniq_index_coll.find_one({"key": cond_value_parsed})
            if uniq_index_entry:
                keys_to_delete_as_ids.append(uniq_index_entry["value"])
                found_in_index = True

        if not found_in_index:
            for doc in collection.find({}):
                doc_id_string = doc["_id"]
                doc_value_string = doc["value"]

                parsed_doc_values = parse_value_string_to_dict(doc_value_string, attributes)
                parsed_doc_values[attributes[0]["name"]] = doc_id_string  # hozzaadjuk az _id-t
                
                current_val_for_comparison = parsed_doc_values.get(cond_field)

                try:
                    if isinstance(cond_value_parsed, int):
                        current_val_for_comparison = int(current_val_for_comparison)
                    elif isinstance(cond_value_parsed, float):
                        current_val_for_comparison = float(current_val_for_comparison)
                    elif isinstance(cond_value_parsed, str):
                        current_val_for_comparison = str(current_val_for_comparison)
                except ValueError:
                    current_val_for_comparison = None

                if current_val_for_comparison is not None and current_val_for_comparison == cond_value_parsed:
                    keys_to_delete_as_ids.append(doc_id_string)

    if not keys_to_delete_as_ids:
        return f"Error: No records found matching condition '{cond_field} = {cond_value_parsed}' in table '{table_name}'."
    
    for _id_to_delete_string in keys_to_delete_as_ids:
        for other_table_info in db["tables"]:
            if other_table_info["name"] == table_name:
                continue

            for other_attr in other_table_info["attributes"]:
                if other_attr["name"] == f"{table_name}_id":
                    referenced_table_name = other_table_info["name"]
                    foreign_key_column_name_in_ref_table = other_attr["name"]
                    fk_index_name_in_ref_table = f"{referenced_table_name}_{foreign_key_column_name_in_ref_table}_index"
                    
                    if fk_index_name_in_ref_table in mongo_db.list_collection_names():
                        fk_index_coll = mongo_db[fk_index_name_in_ref_table]
                        if fk_index_coll.find_one({"key": _id_to_delete_string}) is not None:
                            return f"Error: Cannot delete record with _id '{_id_to_delete_string}' from table '{table_name}' because it is referenced in table '{referenced_table_name}' by '{foreign_key_column_name_in_ref_table}'."
                    else:
                        print("Nem törlöm ki, csinálj rá indexet BOSS.")
                        pass 

    # dokumentumok torlese es indexek frissitese
    deleted_count = 0
    for _id_to_delete_string in keys_to_delete_as_ids:
        doc = collection.find_one({"_id": _id_to_delete_string}) # kereses az _id mezo alapjan
        if not doc:
            continue

        collection.delete_one({"_id": _id_to_delete_string}) # Torles az _id mezo alapjan
        deleted_count += 1

        parsed_doc_values_for_index_deletion = parse_value_string_to_dict(doc["value"], attributes)
        parsed_doc_values_for_index_deletion[attributes[0]["name"]] = _id_to_delete_string # _id hozzaadva a parsolt dictbe

        for attr in attributes:
            if attr["name"] == attributes[0]["name"]: 
                continue
            
            value = parsed_doc_values_for_index_deletion.get(attr["name"])
            if value is None:
                continue

            uniq_name = f"{table_name}_{attr['name']}_uniqindex"
            index_name = f"{table_name}_{attr['name']}_index"

            if uniq_name in mongo_db.list_collection_names():
                mongo_db[uniq_name].delete_one({"key": value})

            if index_name in mongo_db.list_collection_names():
                index_coll = mongo_db[index_name]
                existing = index_coll.find_one({"key": value})
                if existing:
                    index_coll.update_one(
                        {"_id": existing["_id"]}, # az index dokumentum _id-ja alapjan
                        {"$pull": {"value": _id_to_delete_string}}
                    )
                    # aa az eltavolitas utan ures lesz a 'value' lista, toroljuk az index bejegyzest
                    updated_existing = index_coll.find_one({"_id": existing["_id"]})
                    if updated_existing and not updated_existing["value"]: # ha ures lett alista
                        index_coll.delete_one({"_id": updated_existing["_id"]})

    return f"Deleted {deleted_count} record(s) from {table_name} where {cond_field} = {cond_value_parsed}"

def create_index(tokens):
    if len(tokens) < 5 or tokens[-2].upper() != "ON":
        return "Syntax error in CREATE INDEX"

    if tokens[1].upper() == "UNIQUE":
        index_type = "UNIQUE"
        field_name = tokens[3]
        table_name = tokens[5]
    else:
        index_type = "NON_UNIQUE"
        field_name = tokens[2]
        table_name = tokens[4]

    if mongo_db is None:
        return "Error: No database selected"
    
    catalog = load_catalog()
    db = get_current_database(catalog)
    if not db:
        return "Error: No database selected in catalog."
    
    table_info = next((t for t in db["tables"] if t["name"] == table_name), None)
    if not table_info:
        return f"Error: Table '{table_name}' not found in catalog. Cannot create index."
    attributes = table_info["attributes"]

    field_exists = any(attr["name"] == field_name for attr in attributes)
    if not field_exists:
        return f"Error: Field '{field_name}' not found in table '{table_name}'."
    
    if field_name == attributes[0]["name"]:
        return f"Error: Primary key field '{field_name}' cannot be explicitly indexed. It is already indexed as 'key'."

    source_collection = mongo_db[table_name]
    index_collection_name = f"{table_name}_{field_name}_{'uniqindex' if index_type == 'UNIQUE' else 'index'}"

    if index_collection_name in mongo_db.list_collection_names():
        mongo_db.drop_collection(index_collection_name)
    mongo_db.create_collection(index_collection_name)
    index_collection = mongo_db[index_collection_name]

    if index_type == "NON_UNIQUE":
        value_map = {}
        for row in source_collection.find({}):
            _id_string = row["_id"]
            value_string = row["value"]
            
            parsed_value_obj = parse_value_string_to_dict(value_string, attributes)
            
            field_value = parsed_value_obj.get(field_name)

            if field_value is None:
                continue
            
            if field_value not in value_map:
                value_map[field_value] = []
            value_map[field_value].append(_id_string)

        for value, ids in value_map.items():
            try:
                index_collection.insert_one({"key": value, "value": ids})
            except pymongo.errors.PyMongoError as e:
                return f"Error inserting into index collection {index_collection_name}: {e}"
                
    else: # UNIQUE index
        for row in source_collection.find({}):
            _id_string = row["_id"]
            value_string = row["value"]
            
            parsed_value_obj = parse_value_string_to_dict(value_string, attributes)
            
            field_value = parsed_value_obj.get(field_name)

            if field_value is None:
                continue

            if index_collection.find_one({"key": field_value}) is not None:
                return f"Error: Duplicate value '{field_value}' found for UNIQUE index on '{field_name}' in table '{table_name}'. Index creation failed."
            
            try:
                index_collection.insert_one({"key": field_value, "value": _id_string})
            except pymongo.errors.PyMongoError as e:
                return f"Error inserting into unique index collection {index_collection_name}: {e}"

    return f"{'Unique' if index_type == 'UNIQUE' else 'Non-unique'} index created on '{field_name}' in table '{table_name}'"

def parse_condition(cond):
    match = re.match(r'([\w.]+)\s*(=|<=|>=|<|>)\s*(.+)', cond)
    if match:
        field, op, val = match.groups()
        try:
            val = int(val)
        except ValueError:
            try:
                val = float(val)
            except ValueError:
                val = val.strip('"').strip("'")
        return field, op, val
    return None

def execute_indexed_nested_loop_join(all_results, join_clause, db_info, mongo_db):
   
    print("ALGORITHM: Executing Indexed Nested Loop Join")
    join_table_name = join_clause["table"]
    join_table_alias = join_clause["alias"]
    on_condition = join_clause["on_condition"]
    join_table_info = next((t for t in db_info["tables"] if t["name"] == join_table_name), None)

    left_part_on, right_part_on = on_condition["left"], on_condition["right"]
    alias1, field1 = left_part_on.split('.')
    alias2, field2 = right_part_on.split('.')

    if alias1 == join_table_alias:
        inner_alias, inner_field_name = alias1, field1
        outer_alias, outer_field_name = alias2, field2
    else:
        inner_alias, inner_field_name = alias2, field2
        outer_alias, outer_field_name = alias1, field1

    inner_main_coll = mongo_db[join_table_name]
    is_inner_pk_join = (inner_field_name == join_table_info["attributes"][0]["name"])
    
    BATCH_SIZE = 1000
    final_joined_results = []
    
    for i in range(0, len(all_results), BATCH_SIZE):
        current_batch = all_results[i:i + BATCH_SIZE]
        
        keys_to_lookup_int = set()
        for row in current_batch:
            key_val = row.get(f"{outer_alias}.{outer_field_name}")
            if key_val is not None:
                try: keys_to_lookup_int.add(int(key_val))
                except (ValueError, TypeError): continue
        
        if not keys_to_lookup_int: continue

        inner_data_map = {}
        
        if is_inner_pk_join:
            # ha PK-ra JOIN-olunk, a kulcsokat stringge kell alakitani a kereseshez
            keys_for_db_query = [str(k) for k in keys_to_lookup_int]
            inner_docs_cursor = inner_main_coll.find({"_id": {"$in": keys_for_db_query}})
        else:
            # ha masodlagos indexre, akkor a sajat index kollekcionkat hasznaljuk
            inner_index_coll_name = f"{join_table_name}_{inner_field_name}_index"
            inner_index_coll = mongo_db[inner_index_coll_name]
            index_entries = inner_index_coll.find({"key": {"$in": list(keys_to_lookup_int)}})
            
            pks_to_find = []
            for entry in index_entries:
                if isinstance(entry['value'], list): pks_to_find.extend(entry['value'])
                else: pks_to_find.append(entry['value'])
            
            if not pks_to_find: continue
            inner_docs_cursor = inner_main_coll.find({"_id": {"$in": pks_to_find}})

        # a belso sorok feldolgozasa es a hash map epitese
        for inner_doc in inner_docs_cursor:
            parsed_inner_obj = parse_value_string_to_dict(inner_doc["value"], join_table_info["attributes"])
            parsed_inner_obj[join_table_info["attributes"][0]["name"]] = inner_doc["_id"]
            
            # A hash map kulcsat mindig int-nek kezeljuk , legyen konzisztens
            join_key_val = parsed_inner_obj.get(inner_field_name)
            if join_key_val is not None:
                try:
                    map_key = int(join_key_val)
                    if map_key not in inner_data_map: inner_data_map[map_key] = []
                    inner_data_map[map_key].append(parsed_inner_obj)
                except (ValueError, TypeError): continue

        # A batch sorainak osszekapcsolasa a hash mappel
        for outer_row in current_batch:
            outer_join_value = outer_row.get(f"{outer_alias}.{outer_field_name}")
            if outer_join_value is None: continue
            try:
                lookup_key = int(outer_join_value)
                if lookup_key in inner_data_map:
                    for inner_row_data in inner_data_map[lookup_key]:
                        joined_row = outer_row.copy()
                        for k, v in inner_row_data.items():
                            joined_row[f"{join_table_alias}.{k}"] = v
                        final_joined_results.append(joined_row)
            except (ValueError, TypeError): continue
            
    return final_joined_results

def execute_hash_join(all_results, join_clause, db_info, mongo_db):
    print("ALGORITHM: Executing Hash Join")
    join_table_name = join_clause["table"]
    join_table_alias = join_clause["alias"]
    on_condition = join_clause["on_condition"]
    join_table_info = next((t for t in db_info["tables"] if t["name"] == join_table_name), None)

    left_part_on, right_part_on = on_condition["left"], on_condition["right"]
    alias1, field1 = left_part_on.split('.')
    alias2, field2 = right_part_on.split('.')

    if alias1 == join_table_alias:
        inner_alias, inner_field_name = alias1, field1
        outer_alias, outer_field_name = alias2, field2
    else:
        inner_alias, inner_field_name = alias2, field2
        outer_alias, outer_field_name = alias1, field1

    inner_data_map = {}
    inner_collection = mongo_db[join_table_name]
    for inner_doc in inner_collection.find({}):
        _inner_id_string = inner_doc["_id"]
        parsed_inner_value_obj = parse_value_string_to_dict(inner_doc["value"], join_table_info["attributes"])
        parsed_inner_value_obj[join_table_info["attributes"][0]["name"]] = _inner_id_string
        
        join_key_value = parsed_inner_value_obj.get(inner_field_name)
        if join_key_value is not None:
            try:
                map_key = int(join_key_value)
                if map_key not in inner_data_map: inner_data_map[map_key] = []
                inner_data_map[map_key].append(parsed_inner_value_obj)
            except (ValueError, TypeError): continue

    new_all_results = []
    for outer_row in all_results:
        outer_join_value = outer_row.get(f"{outer_alias}.{outer_field_name}")
        if outer_join_value is not None:
            try:
                lookup_key = int(outer_join_value)
                if lookup_key in inner_data_map:
                    for inner_row_data in inner_data_map[lookup_key]:
                        joined_row = outer_row.copy()
                        for k, v in inner_row_data.items():
                            joined_row[f"{join_table_alias}.{k}"] = v
                        new_all_results.append(joined_row)
            except (ValueError, TypeError): continue
    return new_all_results


def select_from_table(tokens):
    parsed_statement = parse_select_statement(tokens)
    if "error" in parsed_statement: return parsed_statement["error"]

    columns_to_project = parsed_statement["columns"]
    aggregate_functions = parsed_statement["aggregate_functions"]
    from_table_name = parsed_statement["from_table"]
    from_table_alias = parsed_statement["from_alias"]
    joins = parsed_statement["joins"]
    where_conditions_raw = parsed_statement["where_conditions"]
    group_by_columns = parsed_statement["group_by_columns"]
    order_by_columns = parsed_statement["order_by_columns"]

    if mongo_db is None: return "Error: No database selected"
    catalog = load_catalog()
    db_info = get_current_database(catalog)
    if not db_info: return "Error: Database not found in catalog"
    main_table_info = next((t for t in db_info["tables"] if t["name"] == from_table_name), None)
    if not main_table_info: return f"Error: Table '{from_table_name}' does not exist."

    mongo_initial_filter = {}
    python_side_where_conditions = []
    for cond_str in where_conditions_raw:
        parsed_cond = parse_condition(cond_str)
        if not parsed_cond: return f"Syntax error in WHERE condition: {cond_str}"
        full_field_spec, op, value = parsed_cond
        field_alias, field_name = (full_field_spec.split(".", 1) if "." in full_field_spec else (from_table_alias, full_field_spec))
        if (field_alias == from_table_alias and field_name == main_table_info["attributes"][0]["name"]):
            if op == "=": mongo_initial_filter["_id"] = str(value)
            elif op == ">": mongo_initial_filter["_id"] = {"$gt": str(value)}
            elif op == "<": mongo_initial_filter["_id"] = {"$lt": str(value)}
            elif op == ">=": mongo_initial_filter["_id"] = {"$gte": str(value)}
            elif op == "<=": mongo_initial_filter["_id"] = {"$lte": str(value)}
        else:
            python_side_where_conditions.append({"field_alias": field_alias, "field_name": field_name, "op": op, "value": value})

    all_results = []
    for doc in mongo_db[from_table_name].find(mongo_initial_filter):
        _id_string = doc["_id"]
        parsed_value_obj = parse_value_string_to_dict(doc["value"], main_table_info["attributes"])
        parsed_value_obj[main_table_info["attributes"][0]["name"]] = _id_string
        row = {}
        for k, v in parsed_value_obj.items():
            row[f"{from_table_alias}.{k}"] = v
        all_results.append(row)

    # query optimizer
    for join_clause in joins:
        join_table_name = join_clause["table"]
        on_condition = join_clause["on_condition"]
        
        left_part_on, right_part_on = on_condition["left"], on_condition["right"]
        alias1, field1 = left_part_on.split('.')
        alias2, field2 = right_part_on.split('.')
        inner_field_name = field1 if alias1 == join_clause["alias"] else field2
        
        inner_table_info = next((t for t in db_info["tables"] if t["name"] == join_table_name), None)
        is_pk_join = (inner_field_name == inner_table_info["attributes"][0]["name"])
        
        inner_index_coll_name = f"{join_table_name}_{inner_field_name}_index"
        has_secondary_index = inner_index_coll_name in mongo_db.list_collection_names()

        if is_pk_join or has_secondary_index:
            print(f"OPTIMIZER: Index found on {join_table_name}.{inner_field_name}. Using Indexed Nested Loop Join.")
            all_results = execute_indexed_nested_loop_join(all_results, join_clause, db_info, mongo_db)
        else:
            print(f"OPTIMIZER: No index found on {join_table_name}.{inner_field_name}. Falling back to Hash Join.")
            all_results = execute_hash_join(all_results, join_clause, db_info, mongo_db)
    
    # WHERE szures
    final_results_after_where = []
    if not python_side_where_conditions:
        final_results_after_where = all_results
    else:
        for row in all_results:
            all_conditions_met = True
            for cond in python_side_where_conditions:
                field_alias, field_name, op, target_value = cond["field_alias"], cond["field_name"], cond["op"], cond["value"]
                current_value = row.get(f"{field_alias}.{field_name}")
                if current_value is None: all_conditions_met = False; break
                try:
                    if isinstance(target_value, int): current_value = int(current_value)
                    elif isinstance(target_value, float): current_value = float(current_value)
                    elif isinstance(target_value, str): current_value = str(current_value)
                except (ValueError, TypeError): all_conditions_met = False; break
                
                if op == "=": is_met = (current_value == target_value)
                elif op == "<": is_met = (current_value < target_value)
                elif op == ">": is_met = (current_value > target_value)
                elif op == "<=": is_met = (current_value <= target_value)
                elif op == ">=": is_met = (current_value >= target_value)
                else: is_met = False
                if not is_met: all_conditions_met = False; break
            if all_conditions_met:
                final_results_after_where.append(row)

    # GROUP BY aggregacio
    if group_by_columns:
        grouped_results = {}

        for row in final_results_after_where:
            group_key_values = []
            for group_col_spec_from_query in group_by_columns:
                actual_key_in_row = group_col_spec_from_query
                if "." not in group_col_spec_from_query:
                    found_key_for_grouping = False
                    for k_in_row in row.keys():
                        if k_in_row.endswith(
                            f".{group_col_spec_from_query}"
                        ):
                            actual_key_in_row = k_in_row
                            found_key_for_grouping = True
                            break
                    if not found_key_for_grouping:
                        if (
                            group_col_spec_from_query
                            == main_table_info["attributes"][0]["name"]
                        ):
                            actual_key_in_row = f"{from_table_alias}.{group_col_spec_from_query}"

                value_for_group_key = row.get(actual_key_in_row)
                group_key_values.append(value_for_group_key)

            group_key = tuple(group_key_values)

            if group_key not in grouped_results:
                grouped_results[group_key] = []
            grouped_results[group_key].append(row)

        aggregated_output_rows = []
        for group_key, rows_in_group in grouped_results.items():
            aggregated_row = {}
            if group_by_columns:
                for i, group_col_spec_from_query in enumerate(
                    group_by_columns
                ):
                    col_name_for_output = group_col_spec_from_query
                    if "." in group_col_spec_from_query:
                        _, col_name_for_output = group_col_spec_from_query.split(
                            ".", 1
                        )

                    aggregated_row[col_name_for_output] = group_key[i]

            for agg_func_info in aggregate_functions:
                func_name = agg_func_info["func"]
                field_spec_for_agg = agg_func_info["field"]

                values_for_aggregation = []
                if field_spec_for_agg == "*":
                    values_for_aggregation = rows_in_group
                else:
                    for r_in_group in rows_in_group:
                        actual_key_for_agg_in_row = field_spec_for_agg
                        if "." not in field_spec_for_agg:
                            found_key_for_agg = False
                            for k_in_row_agg in r_in_group.keys():
                                if k_in_row_agg.endswith(
                                    f".{field_spec_for_agg}"
                                ):
                                    actual_key_for_agg_in_row = k_in_row_agg
                                    found_key_for_agg = True
                                    break
                            if (
                                not found_key_for_agg
                                and field_spec_for_agg
                                == main_table_info["attributes"][0]["name"]
                            ):
                                actual_key_for_agg_in_row = f"{from_table_alias}.{field_spec_for_agg}"

                        val = r_in_group.get(actual_key_for_agg_in_row)
                        if val is not None:
                            try:
                                if func_name in ["MIN", "MAX", "SUM", "AVG"]:
                                    if isinstance(val, str):
                                        if "." in val:
                                            values_for_aggregation.append(
                                                float(val)
                                            )
                                        else:
                                            values_for_aggregation.append(
                                                int(val)
                                            )
                                    elif isinstance(val, (int, float)):
                                        values_for_aggregation.append(val)
                                else:
                                    values_for_aggregation.append(val)
                            except ValueError:
                                pass

                agg_result = None
                if func_name == "COUNT":
                    agg_result = len(values_for_aggregation)
                elif func_name == "SUM":
                    numeric_values_for_sum = [
                        v
                        for v in values_for_aggregation
                        if isinstance(v, (int, float))
                    ]
                    agg_result = (
                        sum(numeric_values_for_sum)
                        if numeric_values_for_sum
                        else 0
                    )
                elif func_name == "AVG":
                    numeric_values_for_avg = [
                        v
                        for v in values_for_aggregation
                        if isinstance(v, (int, float))
                    ]
                    agg_result = (
                        sum(numeric_values_for_avg)
                        / len(numeric_values_for_avg)
                        if numeric_values_for_avg
                        else None
                    )
                elif func_name == "MIN":
                    comparable_values_for_min = [
                        v
                        for v in values_for_aggregation
                        if isinstance(v, (int, float, str))
                    ]
                    agg_result = (
                        min(comparable_values_for_min)
                        if comparable_values_for_min
                        else None
                    )
                elif func_name == "MAX":
                    comparable_values_for_max = [
                        v
                        for v in values_for_aggregation
                        if isinstance(v, (int, float, str))
                    ]
                    agg_result = (
                        max(comparable_values_for_max)
                        if comparable_values_for_max
                        else None
                    )

                agg_column_name = f"{func_name}({field_spec_for_agg})"
                aggregated_row[agg_column_name] = agg_result

            aggregated_output_rows.append(aggregated_row)

        final_results_for_ordering = aggregated_output_rows
    elif aggregate_functions:
        aggregated_row = {}
        for agg_func_info in aggregate_functions:
            func_name = agg_func_info["func"]
            field_spec_for_agg = agg_func_info["field"]

            values_for_aggregation = []
            if field_spec_for_agg == "*":
                values_for_aggregation = final_results_after_where
            else:
                for r_in_group in final_results_after_where:
                    actual_key_for_agg_in_row = field_spec_for_agg
                    if "." not in field_spec_for_agg:
                        found_key_for_agg = False
                        for k_in_row_agg in r_in_group.keys():
                            if k_in_row_agg.endswith(
                                f".{field_spec_for_agg}"
                            ):
                                actual_key_for_agg_in_row = k_in_row_agg
                                found_key_for_agg = True
                                break
                        if (
                            not found_key_for_agg
                            and field_spec_for_agg
                            == main_table_info["attributes"][0]["name"]
                        ):
                            actual_key_for_agg_in_row = f"{from_table_alias}.{field_spec_for_agg}"

                    val = r_in_group.get(actual_key_for_agg_in_row)
                    if val is not None:
                        try:
                            if func_name in ["MIN", "MAX", "SUM", "AVG"]:
                                if isinstance(val, str):
                                    if "." in val:
                                        values_for_aggregation.append(
                                            float(val)
                                        )
                                    else:
                                        values_for_aggregation.append(
                                            int(val)
                                        )
                                elif isinstance(val, (int, float)):
                                    values_for_aggregation.append(val)
                            else:
                                values_for_aggregation.append(val)
                        except ValueError:
                            pass

            agg_result = None
            if func_name == "COUNT":
                agg_result = len(values_for_aggregation)
            elif func_name == "SUM":
                numeric_values_for_sum = [
                    v
                    for v in values_for_aggregation
                    if isinstance(v, (int, float))
                ]
                agg_result = (
                    sum(numeric_values_for_sum)
                    if numeric_values_for_sum
                    else 0
                )
            elif func_name == "AVG":
                numeric_values_for_avg = [
                    v
                    for v in values_for_aggregation
                    if isinstance(v, (int, float))
                ]
                agg_result = (
                    sum(numeric_values_for_avg) / len(numeric_values_for_avg)
                    if numeric_values_for_avg
                    else None
                )
            elif func_name == "MIN":
                comparable_values_for_min = [
                    v
                    for v in values_for_aggregation
                    if isinstance(v, (int, float, str))
                ]
                agg_result = (
                    min(comparable_values_for_min)
                    if comparable_values_for_min
                    else None
                )
            elif func_name == "MAX":
                comparable_values_for_max = [
                    v
                    for v in values_for_aggregation
                    if isinstance(v, (int, float, str))
                ]
                agg_result = (
                    max(comparable_values_for_max)
                    if comparable_values_for_max
                    else None
                )

            agg_column_name = f"{func_name}({field_spec_for_agg})"
            aggregated_row[agg_column_name] = agg_result

        final_results_for_ordering = [aggregated_row]
    else:
        final_results_for_ordering = final_results_after_where

    # ORDER BY rendezes
    if order_by_columns:

        def get_value_for_sort(
            row_item,
            field_spec_to_sort,
            main_table_info_param,
            from_table_alias_param,
        ):
            if any(
                field_spec_to_sort == f"{agg['func']}({agg['field']})"
                for agg in aggregate_functions
            ):
                return row_item.get(field_spec_to_sort)

            value = row_item.get(field_spec_to_sort)
            if value is not None:
                return value

            if "." not in field_spec_to_sort:
                for k_in_row in row_item.keys():
                    if k_in_row.endswith(f".{field_spec_to_sort}"):
                        return row_item.get(k_in_row)

                if (
                    field_spec_to_sort
                    == main_table_info_param["attributes"][0]["name"]
                ):
                    return row_item.get(
                        f"{from_table_alias_param}.{field_spec_to_sort}"
                    )
            return None

        def compare_rows(item1, item2):
            for order_col in order_by_columns:
                field_spec = order_col["field"]
                direction = order_col["direction"]

                val1 = get_value_for_sort(
                    item1, field_spec, main_table_info, from_table_alias
                )
                val2 = get_value_for_sort(
                    item2, field_spec, main_table_info, from_table_alias
                )

                if val1 is None and val2 is None:
                    cmp = 0
                elif val1 is None:
                    cmp = 1 if direction == "ASC" else -1
                elif val2 is None:
                    cmp = -1 if direction == "ASC" else 1
                else:
                    try:
                        if isinstance(val1, (int, float)) and isinstance(
                            val2, (int, float)
                        ):
                            if val1 < val2:
                                cmp = -1
                            elif val1 > val2:
                                cmp = 1
                            else:
                                cmp = 0
                        elif isinstance(val1, str) and isinstance(val2, str):
                            if val1 < val2:
                                cmp = -1
                            elif val1 > val2:
                                cmp = 1
                            else:
                                cmp = 0
                        else:
                            s_val1, s_val2 = str(val1), str(val2)
                            if s_val1 < s_val2:
                                cmp = -1
                            elif s_val1 > s_val2:
                                cmp = 1
                            else:
                                cmp = 0
                    except TypeError:
                        cmp = 0

                if cmp != 0:
                    if direction == "DESC":
                        cmp *= -1
                    return cmp
            return 0

        final_results_for_ordering.sort(key=functools.cmp_to_key(compare_rows))

    # PROJECTION
    output_rows = []
    for row_data in final_results_for_ordering:
        projected_row = {}
        if columns_to_project == ["*"]:
            for k, v in row_data.items():
                output_key = k
                if "." in k and not aggregate_pattern.match(k):
                    output_key = k.split(".", 1)[1]
                projected_row[output_key] = v
        else:
            for col_spec_from_query in columns_to_project:
                output_column_name = col_spec_from_query
                if (
                    "." in col_spec_from_query
                    and not aggregate_pattern.match(col_spec_from_query)
                ):
                    output_column_name = col_spec_from_query.split(".", 1)[1]

                value_to_project = None
                is_aggregate_column = False
                agg_match = aggregate_pattern.match(col_spec_from_query)
                if agg_match:
                    is_aggregate_column = True
                    value_to_project = row_data.get(col_spec_from_query)

                if not is_aggregate_column:
                    value_to_project = row_data.get(col_spec_from_query)

                    if value_to_project is None and "." in col_spec_from_query:
                        alias_less_col_spec = col_spec_from_query.split(
                            ".", 1
                        )[1]
                        value_to_project = row_data.get(alias_less_col_spec)

                    if (
                        value_to_project is None
                        and "." not in col_spec_from_query
                    ):
                        for key_in_row_data in row_data.keys():
                            if key_in_row_data.endswith(
                                f".{col_spec_from_query}"
                            ) and not aggregate_pattern.match(
                                key_in_row_data
                            ):
                                value_to_project = row_data.get(
                                    key_in_row_data
                                )
                                break

                projected_row[output_column_name] = value_to_project
        output_rows.append(projected_row)

    seen = set()
    unique_output = []
    for item in output_rows:
        h = json.dumps(item, sort_keys=True)
        if h not in seen:
            seen.add(h)
            unique_output.append(item)

    return json.dumps(unique_output, indent=2, separators=(",", ":"))

def parse_select_statement(tokens):
    parsed = {
        "columns": [],
        "aggregate_functions": [],
        "from_table": None,
        "from_alias": None,
        "joins": [],
        "where_conditions": [],
        "group_by_columns": [],
        "order_by_columns": [],
    }

    select_index = 1
    from_index = -1
    group_by_index = -1
    order_by_index = -1
    where_index = -1

    # indexek keresese
    for i, token in enumerate(tokens):
        token_upper = token.upper()
        if token_upper == "FROM":
            from_index = i
        elif token_upper == "GROUP" and i + 1 < len(tokens) and tokens[i+1].upper() == "BY":
            group_by_index = i
        elif token_upper == "ORDER" and i + 1 < len(tokens) and tokens[i+1].upper() == "BY":
            order_by_index = i
        elif token_upper == "WHERE":
            where_index = i
    
    if from_index == -1:
        return {"error": "Syntax error: FROM clause missing."}

    # Oszlopok parseolasa (SELECT es FROM kozott)
    columns_raw_tokens = tokens[select_index:from_index]
    
    aggregate_pattern = re.compile(r'(MIN|MAX|AVG|COUNT|SUM)\(([\w*.]+)\)', re.IGNORECASE)
    
    collected_select_list_items = []
    for token_group in columns_raw_tokens: 
        parts = [p.strip() for p in token_group.split(',') if p.strip()]
        collected_select_list_items.extend(parts)

    is_only_star = False
    if len(collected_select_list_items) == 1 and collected_select_list_items[0] == "*":
        parsed["columns"] = ["*"]
        is_only_star = True
    else:
        for item_str in collected_select_list_items:
            parsed["columns"].append(item_str)
            agg_match = aggregate_pattern.match(item_str)
            if agg_match:
                func = agg_match.group(1).upper()
                field_in_agg = agg_match.group(2).strip()
                parsed["aggregate_functions"].append({"func": func, "field": field_in_agg})


    current_token_index = from_index + 1
    from_table_and_alias_raw = []
    
    end_of_from_clause_index = len(tokens)
    possible_next_clauses = ["INNER", "WHERE", "GROUP", "ORDER"]

    for i in range(current_token_index, len(tokens)):
        if tokens[i].upper() in possible_next_clauses:
            end_of_from_clause_index = i
            break
            
    from_table_and_alias_raw = tokens[current_token_index:end_of_from_clause_index]
    
    if not from_table_and_alias_raw:
        return {"error": "Syntax error: FROM clause table name missing or invalid."}

    parsed["from_table"] = from_table_and_alias_raw[0]
    parsed["from_alias"] = from_table_and_alias_raw[1] if len(from_table_and_alias_raw) > 1 else from_table_and_alias_raw[0]
    
    current_token_index = end_of_from_clause_index 

    # JOIN zaradekok
    while current_token_index < len(tokens) and tokens[current_token_index].upper() == "INNER" and \
        current_token_index + 1 < len(tokens) and tokens[current_token_index + 1].upper() == "JOIN":

        join_table_index = current_token_index + 2
        if join_table_index >= len(tokens):
            return {"error": "Syntax error: Missing table name after INNER JOIN."}
        join_table = tokens[join_table_index]

        # Alias keresese a JOIN tablahoz
        join_alias_index = join_table_index + 1
        join_alias = join_table # alapertelmezett alias a tablanev
        if join_alias_index < len(tokens) and tokens[join_alias_index].upper() not in ["ON", "WHERE", "GROUP", "ORDER", "INNER"]:
            join_alias = tokens[join_alias_index]
            current_token_index = join_alias_index # frissitjuk, ha van alias
        else:
            current_token_index = join_table_index # Frissitjuk, ha nincs alias

        on_keyword_index = current_token_index + 1
        if on_keyword_index >= len(tokens) or tokens[on_keyword_index].upper() != "ON":
            return {"error": "Syntax error in JOIN clause: missing ON keyword."}
        
        on_clause_start = on_keyword_index + 1
        on_clause_end = len(tokens)
        for i in range(on_clause_start, len(tokens)):
            if tokens[i].upper() in possible_next_clauses: 
                on_clause_end = i
                break
        
        on_condition_tokens = tokens[on_clause_start:on_clause_end]
        on_condition = " ".join(on_condition_tokens)

        if "=" not in on_condition:
            return {"error": "Syntax error in JOIN ON condition: Expecting '=' operator."}
        
        left_part, right_part = on_condition.split("=", 1)

        parsed["joins"].append({
            "type": "INNER",
            "table": join_table,
            "alias": join_alias,
            "on_condition": {
                "left": left_part.strip(),
                "right": right_part.strip()
            }
        })
        current_token_index = on_clause_end

    # WHERE zaradek
    if where_index != -1 and where_index == current_token_index : # Csak akkor, ha a WHERE a kovetkezo
        where_clause_start = where_index + 1
        where_clause_end = len(tokens)
        for i in range(where_clause_start, len(tokens)):
            if tokens[i].upper() in ["GROUP", "ORDER"]:
                where_clause_end = i
                break
        where_str = " ".join(tokens[where_clause_start:where_clause_end])
        parsed["where_conditions"] = [cond.strip() for cond in where_str.split("AND") if cond.strip()]
        current_token_index = where_clause_end

    # GROUP BY zaradek
    if group_by_index != -1 and group_by_index == current_token_index: # Csak akkor, ha a GROUP BY a kovetkezo
        group_by_start = group_by_index + 2
        group_by_end = len(tokens)
        for i in range(group_by_start, len(tokens)):
            if tokens[i].upper() == "ORDER":
                group_by_end = i
                break
        group_by_cols_raw = tokens[group_by_start:group_by_end]
        parsed["group_by_columns"] = []
        for col_token in group_by_cols_raw:
            for col_spec in col_token.split(","):
                cleaned_col_spec = col_spec.strip()
                if cleaned_col_spec:
                    parsed["group_by_columns"].append(cleaned_col_spec)

        current_token_index = group_by_end

    # ORDER BY zaradek
    if order_by_index != -1 and order_by_index == current_token_index: # Csak akkor, ha az ORDER BY a kovetkezo
        order_by_start = order_by_index + 2
        order_by_raw_columns_tokens = tokens[order_by_start:]
        
        i = 0
        while i < len(order_by_raw_columns_tokens):
            col_spec = order_by_raw_columns_tokens[i].strip().rstrip(',') 
            if not col_spec:
                i += 1
                continue

            direction = "ASC"
            if i + 1 < len(order_by_raw_columns_tokens):
                next_token_upper = order_by_raw_columns_tokens[i+1].upper().rstrip(',')
                if next_token_upper == "ASC" or next_token_upper == "DESC":
                    direction = next_token_upper
                    i += 1 
            
            parsed["order_by_columns"].append({"field": col_spec, "direction": direction})
            i += 1
            
    has_regular_columns = False
    has_aggregate_functions_in_select = False
    for col in parsed["columns"]:
        if col == "*": # * esete
            continue
        agg_match = aggregate_pattern.match(col)
        if agg_match:
            has_aggregate_functions_in_select = True
        else:
            has_regular_columns = True

    # Ha vannak aggregacios fuggv a SELECT-ben
    if has_aggregate_functions_in_select:
        if has_regular_columns and not parsed["group_by_columns"]:
            for col_in_select in parsed["columns"]:
                if not aggregate_pattern.match(col_in_select) and col_in_select != "*":
                    if col_in_select not in parsed["group_by_columns"]:
                        return {"error": f"Column '{col_in_select}' in SELECT list is not an aggregate function and not in GROUP BY clause."}

        if not parsed["group_by_columns"] and has_regular_columns:
            # itt az a lenyeg, hogy ha a SELECT-ben van "mezo" ÉS "aggregalt_mezo", akkor kell GROUP BY.
            # DE a SELECT COUNT(*) FROM table_name is elfogadhato, ahol csak aggregalt fuggveny van.
            # Itt szigorubb ellenorzes szukseges, hogy csak aggregatumok lehetnek group by nelkul
            if any(not aggregate_pattern.match(col) and col != "*" for col in parsed["columns"]):
                 return {"error": "Cannot mix aggregate functions with non-aggregated columns without a GROUP BY clause."}

    return parsed

def insert_bulk_into_table(tokens):
    if (
        len(tokens) < 4
        or tokens[2].upper() != "INTO"
        or tokens[4].upper() != "VALUES"
    ):
        return "Syntax error in INSERT BULK"

    table_name = tokens[3]

    values_string_start_idx = tokens.index("VALUES") + 1
    values_string = " ".join(tokens[values_string_start_idx:])

    record_strings = re.findall(r"\(([^)]*)\)", values_string)

    documents_to_insert = []
    for record_str in record_strings:
        tokens_for_record = re.findall(r'"[^"]*"|\S+', record_str)
        documents_to_insert.append(tokens_for_record)

    try:
        inserted_count = parse_and_insert_documents(
            table_name, documents_to_insert, skip_index_update=True
        )
        return f"Inserted {inserted_count} records into {table_name}"
    except ValueError as ve:
        return f"Error: {str(ve)}"
    except Exception as e:
        return f"Unexpected error: {str(e)}"
    
def parse_and_insert_documents(table_name, records_values_list, skip_index_update=False):
    if mongo_db is None:
        raise ValueError("Error: No database selected")
    
    catalog = load_catalog()
    db = get_current_database(catalog)
    table = next((t for t in db["tables"] if t["name"] == table_name), None)
    if not table:
        raise ValueError(f"Table '{table_name}' does not exist in the current database.")
    
    attributes = table["attributes"]
    documents_for_mongo = []

    for values_raw in records_values_list:
        if len(values_raw) != len(attributes):
            raise ValueError(f"Expected {len(attributes)} values, got {len(values_raw)}")

        converted_values = []
        for attr, val_str in zip(attributes, values_raw):
            typ = attr["type"]
            try:
                if typ == "int":
                    val = int(val_str)
                elif typ == "float":
                    val = float(val_str)
                elif typ == "str":
                    val = val_str.strip('"').strip("'")
                else:
                    raise ValueError(f"Unsupported type: {typ}")
            except ValueError:
                raise ValueError(f"Type mismatch for '{attr['name']}': expected {typ}, got '{val_str}'")
            converted_values.append(val)

        _id_value = str(converted_values[0])
        value_string = "#".join(str(v) for v in converted_values[1:])
        documents_for_mongo.append({"_id": _id_value, "value": value_string})

    if not documents_for_mongo:
        return 0

    try:
        collection = mongo_db[table_name]
        collection.insert_many(documents_for_mongo, ordered=False)

        if skip_index_update:
            return len(documents_for_mongo)

        return len(documents_for_mongo)

    except pymongo.errors.BulkWriteError as bwe:
        error_msg = f"Bulk write error: {bwe.details}"
        print(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Unexpected error during bulk insert: {str(e)}"
        print(error_msg)
        raise ValueError(error_msg)

# Socket szerver
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen()
    print(f"The server started at {HOST}:{PORT} address...")
    while True:
        conn, addr = s.accept()
        with conn:
            print(f"Connected: {addr}")
            buffer = ""
            while True:
                try:
                    data = conn.recv(4096).decode()
                except ConnectionResetError:
                    print(f"Client {addr} forcefully disconnected.")
                    break
                except Exception as e:
                    print(f"Error receiving data: {e}")
                    break

                if not data:
                    print(f"Client {addr} disconnected.")
                    break

                buffer += data

                while "\n" in buffer:
                    command, buffer = buffer.split("\n", 1)
                    command = command.strip()

                    if not command:
                        continue

                    print(f"Command: {command}")
                    try:
                        result = process_command(command)
                    except Exception as e:
                        print(f"Error during command processing: {e}")
                        result = f"Error: {e}"

                    try:
                        conn.sendall((result + "\n<<END>>\n").encode())
                    except Exception as e:
                        print(f"Error sending response back to the client {addr}: {e}")
                        break
            print(f"Connection closed: {addr}")
