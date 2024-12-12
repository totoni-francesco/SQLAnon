from sqltabs_local import *
import sqlite3
import logging

try:
    # Connessione al database
    db_name = "dataset.db" 
    logging.info("Connessione al database: %s", db_name)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Parsing dei valori da script.conf
    logging.info("Parsing dei valori dal file di configurazione.")
    columnnames = parse_specific_value("script.conf", "columnname")
    randomtypes = parse_specific_value("script.conf", "randomtype")
    schemanames = parse_specific_value("script.conf", "nameschema")
    tablenames = parse_specific_value("script.conf", "nametable")
    linkednames = parse_specific_value("script.conf", "linked_to")
    
    completenames = [str(a) + "." + str(b) for a, b in zip(schemanames, tablenames)]

    # Lettura dei nomi delle colonne
    lines = get_column_names(completenames)

    # Creazione del dataset
    create_dataset(lines, conn, cursor)

    # Inserimento dei valori nel dataset
    dataset_insert(lines, cursor, randomtypes, columnnames, completenames, linkednames)

    # Commit delle modifiche
    conn.commit()
    logging.info("Modifiche salvate con successo.")

except Exception as e:
    logging.error("Errore durante l'esecuzione del codice: %s", str(e))

finally:
    # Chiusura della connessione al database
    if conn:
        conn.close()
        logging.info("Connessione al database chiusa.")