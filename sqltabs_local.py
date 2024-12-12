import sqlite3
import subprocess
import re
import csv
import logging
import traceback
import random
import string
from io import StringIO
from faker import Faker
fake=Faker()

logging.basicConfig(level=logging.DEBUG, filename="py_log.log", filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

db_name = "dataset.db"  #Nome del dataset in cui verranno inserite le modifiche
sql_name = "Sql_name"  #Nome del dump da modificare
masking_table = {}


def extract_balanced_parentheses(line):
    #Prende valori presenti tra parentesi
    stack = []
    start_index = -1
    
    for i, char in enumerate(line):
        if char == '(':
            if start_index == -1:
                start_index = i
            stack.append(char)
        
        elif char == ')':
            stack.pop()
            if not stack:
                return start_index, i
    
    logging.error("Parentesi non bilanciate nella stringa: %s", line)
    raise ValueError("Parentesi non bilanciate nella stringa.")


def get_column_names(table):
    #Vengono prese le colonne dalla sorgente
    app = []
    lines = []
    spotted = False
    i = 0

    logging.info("Inizio ricerca delle tabelle")
    
    try:
        with open(sql_name, 'r', encoding='utf-8') as infile, open('try.sql', 'w', encoding='utf-8') as outfile:
            for line in infile:
                outfile.write(line)
                for tbl in table:
                    if line.strip().startswith(f"CREATE TABLE {tbl} "):
                        i += 1
                        spotted = True
                        current_table = tbl
                        logging.info("Tabella trovata: %s", current_table)

                if spotted and not line.strip().endswith(");") and not any(line.strip().startswith(f"CREATE TABLE {tbl} ") for tbl in table):
                    app.append(line.strip())

                if spotted and line.strip().endswith(");"):
                    lines.append(str({current_table}))
                    lines.append(app.copy())
                    logging.info("Colonne estratte per la tabella %s: %s", current_table, app)
                    app.clear()
                    spotted = False

                    if i >= len(table):
                        logging.info("Ricerca delle tabelle completata.")
                        break

    except Exception as e:
        logging.error("Errore durante la ricerca delle colonne: %s", str(e))
    
    return lines


def parse_specific_value(file_path, field_name):
#Viene letto lo script riga per riga
    collection = []
    pattern = r"<(.*?)>"

    try:
        with open(file_path, 'r') as file:
            content = file.read()

        matches = re.findall(pattern, content)

        for match in matches:
            fields = match.split(";")
            parsed_data = {
                "linked_to": "",         # Valore di default
                "nameschema": "public",  # Valore di default
                "randomtype": "line"     # Valore di default
            }
            seen_keys = set()

            for field in fields:
                key_value = field.split(":")
                if len(key_value) == 2:
                    key = key_value[0].strip()

                    # Controllo per evitare duplicati
                    if key in seen_keys:
                        logging.error("Duplicato rilevato per il campo '%s' all'interno di un blocco: %s", key, match)
                        raise ValueError(f"Duplicato rilevato per il campo '{key}' all'interno di un blocco < >.")
                    
                    seen_keys.add(key)
                    value = key_value[1].strip().strip('"')
                    parsed_data[key] = value

            # Controllo dei campi obbligatori
            if "nametable" not in parsed_data or "columnname" not in parsed_data:
                logging.error("Mancano i campi obbligatori 'nametable' o 'columnname' nel blocco: %s", match)
                raise ValueError("Mancano i campi obbligatori 'nametable' o 'columnname'")

            # Aggiunge il valore richiesto alla collezione
            if field_name in parsed_data:
                collection.append(parsed_data[field_name])

    except Exception as e:
        logging.error("Errore durante il parsing del file %s: %s", file_path, str(e))
    
    return collection


def create_dataset(lines, conn, cursor):
#Crea o aggiorna tabelle nel database SQLite.
    for i in range(0, len(lines), 2):
        table_name = lines[i].strip("{}").strip("'")
        values = lines[i + 1]
        columns = [col.strip() for col in values]
        columns_definition = ''.join(columns)

        mask_columns_definition = []
        for col in columns:
            parole = col.split(" ", 1)
            prima_parola_modificata = parole[0] + "_mask"
            nuova_colonna = prima_parola_modificata + " " + parole[1] if len(parole) > 1 else prima_parola_modificata
            mask_columns_definition.append(nuova_colonna)
        mask_columns_definition = "".join(mask_columns_definition)

        if "." in table_name:
            fake_table_name = table_name.replace(".", "_")

        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (fake_table_name,))
        table_exists = cursor.fetchone() is not None

        if not table_exists:
            try:
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {fake_table_name} (
                        {columns_definition},
                        {mask_columns_definition}
                    );
                """)
                logging.info("Tabella %s creata con successo.", fake_table_name)
            except Exception as e:
                logging.error("Errore durante la creazione della tabella %s: %s", fake_table_name, e)
                conn.rollback()
        else:
            cursor.execute(f"PRAGMA table_info({fake_table_name});")
            existing_columns = [info[1] for info in cursor.fetchall()]

            for col in columns:
                col_name = col.split()[0]
                if col_name not in existing_columns:
                    try:
                        cursor.execute(f"ALTER TABLE {fake_table_name} ADD COLUMN {col};")
                        logging.info("Colonna '%s' aggiunta alla tabella %s.", col_name, fake_table_name)
                    except Exception as e:
                        logging.error("Errore durante l'aggiunta della colonna %s alla tabella %s: %s", col_name, fake_table_name, e)
                        conn.rollback()


def dataset_insert(lines, cursor, rtype, columnnames, completenames, links):
#Vengono inseriti i dati nel dataset
    logging.info("Inizio dell'inserimento dei valori.")
    copy_found = False

    with open(sql_name, 'r', encoding='utf-8') as infile, open('try.sql', 'a', encoding='utf-8') as outfile:
        for line in infile:
            spotted = ''

            for names in completenames:
                if line.startswith(f"INSERT INTO {names} "):
                    spotted = "Insert"
                if line.startswith(f"COPY {names} ") or copy_found == True:
                    spotted = "Copy"

            match spotted:
                case "Insert":
                    try:
                        start_index = line.index('(') + 1
                        end_index = line.index(');')
                        values = line[start_index:end_index].strip()
                        for i in range(0, len(lines), 2):
                            table_name = lines[i].strip("{}").strip("'")
                            table_column = lines[i + 1]
                            columns = get_clean_column(table_column)
                            columns_list = columns.split(", ")

                            if table_name in line:
                                rnd_values = random_values(cursor, line, rtype, columnnames, completenames, columns, links)
                                if "." in table_name:
                                    fake_table_name = table_name.replace(".", "_")

                                pattern = r"(?:\d+)|(?:'(?:''|[^'])*')|(?:NULL)|(?:'')"
                                matches = re.findall(pattern, values)
                                result = [match.strip("'") if match not in ("NULL", "''") else match for match in matches]

                                conc = ''
                                for index, column in enumerate(columns_list):
                                    if index != 0:
                                        conc += " AND "
                                    if (result[index] != "NULL") and (result[index] != "''"):
                                        if not result[index].isdigit():
                                            result[index] = f"'{result[index]}'"
                                        conc += f"{column} = {result[index]}"
                                    else:
                                        if result[index] == "''":
                                            conc += f"{column} = ''"
                                        if result[index] == "NULL":
                                            conc += f"{column} IS NULL"

                                check = f"SELECT * FROM {fake_table_name} WHERE {conc};"
                                cursor.execute(check)
                                fetch = cursor.fetchone()

                                if fetch is None:
                                    query = f"INSERT INTO {fake_table_name} VALUES ({values}, {rnd_values});"
                                    cursor.execute(query)
                                    replacement = replace(line, cursor, table_column, fake_table_name)
                                    outfile.write(replacement)
                                else:
                                    fetch = ["NULL" if value is None else value for value in fetch]
                                    fetch = tuple(fetch)
                                    half_index = len(fetch) // 2
                                    right_half = fetch[half_index:]
                                    query = f"INSERT INTO {table_name} VALUES {right_half};\n"
                                    if "'NULL'" in query:
                                        query = query.replace("'NULL'", "NULL")
                                    outfile.write(query)
                    except Exception as e:
                        logging.error(f"Errore durante l'elaborazione della riga INSERT: {e}")

                case "Copy":
                    try:
                        fake_columns = []
                        if line.startswith('COPY'):
                            copy_found = True
                            match = re.match(r"COPY\s+(\S+)\s+\((.*?)\)\s+(.*)", line)
                            if match:
                                table = match.group(1)
                                columns = match.group(2).split(",")
                                columns_str = [col.strip() for col in columns]

                                if "." in table:
                                    fake_table = table.replace(".", "_")
                                for column in columns_str:
                                    fake_columns.append(column + "_mask")

                                outfile.write(line)

                        if line.startswith('\\.'):
                            copy_found = False
                            outfile.write(line)

                        if copy_found == True and line.startswith('COPY') == False:
                            lista = line.split("\t")
                            lista = [elemento if elemento != "" else None for elemento in lista]
                            for i in range(0, len(lines), 2):
                                table_name = lines[i].strip("{}").strip("'")
                                table_column = lines[i + 1]
                                if table_name in table:
                                    new_line = random_values(cursor, lista, rtype, columnnames, completenames, columns_str, links)
                                    mod_line = (str(line).rstrip('\n') + "\t" + str(new_line).rstrip('\n'))

                                    ric_line = get_value(mod_line, fake_table, columns, cursor)
                                    outfile.write(ric_line)
                    except Exception as e:
                        logging.error(f"Errore durante l'elaborazione della riga COPY: {e}")

                case _:
                    outfile.write(line)

    logging.info("Inserimento e/o modifica completate.")


def random_values(cursor, line, rtype, columnnames, completenames, col, links):
#Funzione per la randomizzazione
    try:
        check=''
        if isinstance(line, str):   #Se si tratta di un INSERT INTO
            col_list = col.split(", ")
            start_index, end_index = extract_balanced_parentheses(line)
            values_str = line[start_index + 1:end_index].strip()
            reader = csv.reader(StringIO(values_str), skipinitialspace=True, quotechar="'")
            values = next(reader)

            values = []
            current_value = ""
            in_quotes = False

            for char in values_str:
                if char == "'" and (not current_value or current_value[-1] != "\\"):
                    in_quotes = not in_quotes
                    current_value += char
                elif char == "," and not in_quotes:
                    values.append(current_value.strip())
                    current_value = ""
                else:
                    current_value += char
            if current_value:
                values.append(current_value.strip())

            for randomtype, column, names, link in zip(rtype, columnnames, completenames, links):
                if line.startswith(f"INSERT INTO {names} "):
                    if column in col_list:
                        position = col_list.index(column)
                        original_value = values[position]

                        if link != "":
                            parts = link.split(")")
                            parts[0] = parts[0].strip("(")
                            parts = [part.strip() for part in parts]
                            if "." in parts[0]:
                                parts[0] = parts[0].replace(".", "_")
                            table_link= parts[0]
                            column_link= parts[1]
                            cursor.execute(f"SELECT {column_link}_mask FROM {table_link} WHERE {column_link}={values[position]}")
                            check= cursor.fetchone()
                            if check is None:
                                check=''

                        if check == '':
                            if original_value not in [None, '', 'NULL', r'\N']:
                                new_value=rnd_type(original_value, randomtype, fake)
                                query = f"SELECT 1 FROM {names.replace('.', '_')} WHERE {column}_mask = ?"
                                cursor.execute(query, (new_value,))

                                while cursor.fetchone():
                                    new_value=rnd_type(original_value, randomtype, fake)
                                

                                if original_value.startswith("'") and original_value.endswith("'"):
                                    values[position] = f"'{new_value}'"
                                else:
                                    values[position] = new_value
                        else:
                            if original_value not in [None, '', 'NULL', r'\N']:
                                values[position] = "'"+check[0]+"'"

            values_str = ", ".join(values)
    
        else:   #Se si tratta di un COPY
            for randomtype, column, names, link in zip(rtype, columnnames, completenames, links):
                if column in col:
                    position = col.index(column)
                    original_value = line[position]
                    if link != "":
                        parts = link.split(")")
                        parts[0] = parts[0].strip("(")
                        parts = [part.strip() for part in parts]
                        if "." in parts[0]:
                            parts[0] = parts[0].replace(".", "_")
                        table_link= parts[0]
                        column_link= parts[1]
                        cursor.execute(f"SELECT {column_link}_mask FROM {table_link} WHERE {column_link}='{original_value}'")
                        check= cursor.fetchone()
                        if check is None:
                            check=''


                    if check == '':
                        if original_value not in [None, '', 'NULL', r'\N']:
                            new_value=rnd_type(original_value, randomtype, fake)

                            query = f"SELECT 1 FROM {names.replace('.', '_')} WHERE {column}_mask = ?"
                            cursor.execute(query, (new_value,))

                            while cursor.fetchone():
                                new_value=rnd_type(original_value, randomtype, fake)

                            line[position] = new_value
                    else:
                        if original_value not in [None, '', 'NULL', r'\N']:
                            line[position] = check[0]

            values_str = "\t".join(
                str(value) if value not in [None, False] else "" for value in line
            )   

        return values_str

    except Exception as e:
        logging.error(f"Errore nella funzione random_values: {e}")
        raise


def rnd_type(value, randomtype, fake):
#Funzione per aggiungere altri tipi di randomizzazione
    match randomtype:
        case 'line':
            randomized = []
            if value.startswith("'") and value.endswith("'"):
                value=value[1:-1]
            for char in value:
                if char.islower():  # Se è una lettera minuscola
                    randomized.append(random.choice(string.ascii_lowercase))
                elif char.isupper():  # Se è una lettera maiuscola
                    randomized.append(random.choice(string.ascii_uppercase))
                else:  # Mantieni invariati numeri e caratteri speciali
                    randomized.append(char)
            return ''.join(randomized)
        
        case 'ip':
            randomized = []
            if value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            for char in value:
                if char.isdigit():  # Se è un numero
                    randomized.append(str(random.randint(0, 9)))
                else:  # Mantieni invariati lettere e simboli
                    randomized.append(char)
            return ''.join(randomized)
        
        case _:
            new_value = getattr(fake, randomtype)()
            return new_value


def get_clean_column(table_column):
#Funzione per ricavare solamente i nomi delle colonne
    try:
        colu = [col.split()[0] for col in table_column]
        nuova_columns_definition = []
        for col in colu:
            parole = col.split(" ", 1)  # Dividi solo alla prima occorrenza di spazio
            prima_parola_modificata = parole[0]  # Mantieni la prima parola invariata
            nuova_colonna = prima_parola_modificata + " " + parole[1] if len(parole) > 1 else prima_parola_modificata
            nuova_columns_definition.append(nuova_colonna)

        nuova_columns_definition = ", ".join(nuova_columns_definition)
        return nuova_columns_definition

    except Exception as e:
        logging.error(f"Errore nella funzione get_clean_column: {e}")
        raise


def replace(line, cursor, columns, name):
#Funzione per rimpiazzare la linea originale con quella fittizia
    try:
        # Ottieni i nomi delle colonne originali e delle colonne modificate
        column = [s.split()[0] if s.strip() else '' for s in columns]
        fcolumn = [s + "_mask" for s in column]

        # Prepara la lista di colonne modificate
        fclean_columns = ", ".join(fcolumn)

        # Estrai i valori dalla riga SQL originale
        start_index, end_index = extract_balanced_parentheses(line)
        values_str = line[start_index + 1:end_index].strip()
        reader = csv.reader(StringIO(values_str), skipinitialspace=True, quotechar="'")
        values = next(reader)

        # Esegui la query per ottenere i valori modificati
        query = f"SELECT {fclean_columns} FROM {name} WHERE {column[0]} = ?"
        cursor.execute(query, (values[0],))
        riga_modificata = cursor.fetchone()

        if riga_modificata:
            # Costruisci la stringa con i valori modificati
            valori_modificati = []
            for val in riga_modificata:
                if val is None:
                    valori_modificati.append("NULL")  # Converte None in NULL
                elif isinstance(val, str):
                    # Gestisce le stringhe con escaping delle virgolette singole
                    escaped_val = val.replace("'", "''")
                    valori_modificati.append(f"'{escaped_val}'")
                else:
                    valori_modificati.append(str(val))  # Mantiene numeri e altri tipi come stringa

            # Crea una stringa finale con i valori modificati
            riga_modificata_str = ", ".join(valori_modificati)

            # Sostituisci i valori nella stringa originale
            line = line[:start_index + 1] + riga_modificata_str + line[end_index:]

        return line

    except Exception as e:
        logging.error(f"Errore nella funzione replace: {e}")
        raise


def transform(lista, table, cursor):
#Viene creata la riga da eseguire con SQLite
    try:
        # Modifica gli elementi della lista sostituendo None con NULL e aggiungendo gli apici
        for i in range(len(lista)):
            lista[i] = f"'{lista[i]}'" if lista[i] is not None else "NULL"

        new_line = "\t".join(lista)

        # Gestisce il caso in cui ci siano tabulazioni e "\N"
        if "\t" in new_line:
            new_line = new_line.replace("\t", ", ")
        if "\\N" in new_line:
            new_line = new_line.replace("\\N", "NULL")
        if "'None'" in new_line:
            new_line = new_line.replace("'None'", "''")
        new_line = new_line.replace("\n", "")

        # Costruisce la nuova riga da eseguire nel database
        full_new_line = f"INSERT INTO {table} VALUES ({new_line});\n"
        cursor.execute(full_new_line)

        # Ottieni la metà destra della lista e rimuovi gli apici dove necessario
        half_index = len(lista) // 2
        right_half = lista[half_index:]
        right_half = [word[1:-1] if word.startswith("'") and word.endswith("'") else word for word in right_half]
        lista_string = "\t".join(right_half) + "\n"
        
        return lista_string

    except Exception as e:
        logging.error(f"Errore nella funzione transform: {e}")
        raise


def get_value(line, table, columns, cursor):
#Se già presenti, ricava i valori dal dataset
    try:
        conc = ''
        lista = line.split("\t")
        lista = [elemento.replace("'", "''") if elemento is not None else 'NULL' for elemento in lista]

        # Crea la parte della query con i parametri corretti
        for index, column in enumerate(columns):
            if index != 0:
                conc += " AND "

            if (lista[index] != '') and (lista[index] != r"\N"):
                conc += f"{column} = '{lista[index]}'"
            else:
                if lista[index] == r"\N":
                    conc += f"{column} = 'NULL'"
                if lista[index] == '':
                    conc += f"{column} = ''"

        # Esegui la query di selezione
        check = f"SELECT * FROM {table} WHERE {conc};"
        cursor.execute(check)
        fetch = cursor.fetchone()

        # Se il record esiste, ricava i valori
        if fetch:
            fetch = list(fetch)
            for i, f in enumerate(fetch):
                if f == '':
                    fetch[i] = ''
                elif f == 'NULL':
                    fetch[i] = r"\N"

            half_right = fetch[len(fetch) // 2:]
            fetch_string = "\t".join(str(x) for x in half_right) + '\n'
            return fetch_string

        # Se il record non esiste, usa i nuovi valori e inserisce tutto
        else:
            new = transform(lista, table, cursor)
            return new

    except Exception as e:
        logging.error(f"Errore nella funzione get_value per la linea: {line}. Dettaglio errore: {e}")
        logging.error("Traceback completo:\n" + traceback.format_exc())
        raise
