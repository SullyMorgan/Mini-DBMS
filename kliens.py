import json
import socket
import os
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion

# Feltételezzük, hogy a client_utils.py létezik és tartalmazza a szükséges függvényt.
from client_utils import send_command_via_existing_socket

HOST = "localhost"
PORT = 65432

current_db = None


class SQLCompleter(Completer):
    def __init__(self, keywords, databases, tables, fields):
        self.main_keywords = keywords
        self.databases = databases
        self.tables = tables
        self.fields = fields
        self.aggregates = ["SUM(", "COUNT(", "AVG(", "MIN(", "MAX("]
        self.logical_operators = ["AND", "OR"]
        self.order_by_keywords = ["ASC", "DESC"]

    def get_completions(self, document, complete_event):
        text_before_cursor = document.text_before_cursor
        text_upper = text_before_cursor.upper()
        word_before_cursor = document.get_word_before_cursor(WORD=True)
        char_before_cursor = document.char_before_cursor

        tokens = text_upper.split()
        if not tokens:
            for keyword in self.main_keywords:
                if keyword.startswith(word_before_cursor.upper()):
                    yield Completion(keyword, -len(word_before_cursor))
            return

        main_cmd = tokens[0]
        suggestions = set()

        if main_cmd == "SELECT":
            from_idx = text_upper.rfind(" FROM ")
            where_idx = text_upper.rfind(" WHERE ")
            groupby_idx = text_upper.rfind(" GROUP BY ")
            orderby_idx = text_upper.rfind(" ORDER BY ")

            last_clause_idx = max(from_idx, where_idx, groupby_idx, orderby_idx)

            if orderby_idx > -1 and orderby_idx == last_clause_idx:
                if char_before_cursor.isspace():
                    suggestions.update(self.order_by_keywords)
                    suggestions.add(",")
                else:
                    suggestions.update(self.fields)
            elif groupby_idx > -1 and groupby_idx == last_clause_idx:
                if char_before_cursor.isspace():
                    suggestions.add("ORDER BY")
                    suggestions.add(",")
                else:
                    suggestions.update(self.fields)
            elif where_idx > -1 and where_idx == last_clause_idx:
                last_token = tokens[-1] if tokens else ""
                if last_token == "WHERE" or last_token in self.logical_operators:
                    suggestions.update(self.fields)
                elif char_before_cursor.isspace():
                    suggestions.update(self.logical_operators)
                    suggestions.add("GROUP BY")
                    suggestions.add("ORDER BY")
                else:
                    suggestions.update(self.fields)
            elif from_idx > -1 and from_idx == last_clause_idx:
                # JAVÍTOTT, JOIN-T IS KEZELŐ LOGIKA
                last_token = tokens[-1] if tokens else ""
                second_last_token = tokens[-2] if len(tokens) > 1 else ""

                if last_token == "FROM" or (
                    second_last_token == "INNER" and last_token == "JOIN"
                ):
                    suggestions.update(self.tables)
                elif last_token == "ON":
                    suggestions.update(self.fields)
                elif char_before_cursor.isspace():
                    suggestions.add("WHERE")
                    suggestions.add("GROUP BY")
                    suggestions.add("ORDER BY")
                    suggestions.add("INNER JOIN")
                    suggestions.add("ON")
                else:
                    suggestions.update(self.tables)
            else:
                if text_before_cursor.strip().endswith(","):
                    suggestions.update(self.fields)
                    suggestions.update(self.aggregates)
                    suggestions.add("*")
                elif char_before_cursor.isspace() and len(tokens) > 1:
                    suggestions.add("FROM")
                    suggestions.add(",")
                else:
                    suggestions.update(self.fields)
                    suggestions.update(self.aggregates)
                    suggestions.add("*")

        elif main_cmd in ("DROP", "CREATE"):
            if len(tokens) > 1:
                sub_cmd = tokens[1]
                if sub_cmd == "DATABASE":
                    suggestions.update(self.databases)
                elif sub_cmd == "TABLE":
                    suggestions.update(self.tables)
            else:
                suggestions.add("DATABASE")
                suggestions.add("TABLE")
                suggestions.add("INDEX")
                suggestions.add("UNIQUE")
        elif main_cmd == "USE":
            suggestions.update(self.databases)
        else:
            suggestions.update(self.main_keywords)

        for suggestion in sorted(list(suggestions)):
            if suggestion.upper().startswith(word_before_cursor.upper()):
                yield Completion(suggestion, -len(word_before_cursor))


def get_completer():
    global current_db
    main_keywords = [
        "SELECT", "INSERT", "DELETE", "UPDATE", "CREATE", "DROP", "USE",
    ]

    tables = []
    fields = []
    databases = []

    catalog_path = "catalog.json"
    if os.path.exists(catalog_path):
        try:
            with open(catalog_path, "r") as f:
                catalog = json.load(f)
                for db in catalog.get("databases", []):
                    db_name = db["name"]
                    databases.append(db_name)
                    if current_db == db_name:
                        for t in db.get("tables", []):
                            tables.append(t["name"])
                            for attr in t.get("attributes", []):
                                fields.append(f"{t['name']}.{attr['name']}")
                                fields.append(attr["name"])

        except (json.JSONDecodeError, Exception) as e:
            print(
                f"Warning: Could not load catalog.json: {e}. Autocompletion might be limited."
            )
    else:
        print(
            "Warning: catalog.json not found. Autocompletion might be limited."
        )

    return SQLCompleter(main_keywords, databases, tables, list(set(fields)))


def main():
    global current_db
    session = PromptSession()

    cli_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        cli_socket.connect((HOST, PORT))
        print(f"Successfully connected to server at {HOST}:{PORT}")
    except ConnectionRefusedError:
        print(
            f"Error: Could not connect to server at {HOST}:{PORT}. Is the server running?"
        )
        return
    except Exception as e:
        print(f"Error connecting to server: {e}")
        return

    try:
        while True:
            completer = get_completer()
            try:
                cmd = session.prompt("COMMAND> ", completer=completer)
            except KeyboardInterrupt:
                print("\nPress Ctrl+D or type 'exit' to exit.")
                continue
            except EOFError:
                break

            if not cmd.strip():
                continue

            if cmd.lower() == "exit":
                break

            response_from_server = send_command_via_existing_socket(
                cli_socket, cmd
            )

            if cmd.upper().startswith("USE "):
                parts = cmd.split(" ", 1)
                if len(parts) > 1:
                    db_name_attempt = parts[1].strip()
                    if response_from_server.startswith("Using database:"):
                        current_db = db_name_attempt

            print("Response from server:", response_from_server)

    except Exception as e:
        print(f"An unexpected error occurred in the client: {e}")
    finally:
        print("\nClosing connection to server.")
        cli_socket.close()


if __name__ == "__main__":
    main()