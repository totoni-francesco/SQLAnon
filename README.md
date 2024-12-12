# SQLAnon

**_Lo script presenta 5 campi (2 obbligatori da specificare e 3 con valori di default)_**

*Obbligatorie:*

- nametable (Nome della tabella) 
- columnname (Nome della colonna nella quale apportare modifiche)

*Non obbligatorie:*

- nameschema (Nome dello schema, di default impostato come public)
- randomtype (Tipologia di anonimizzazione utilizzando il package Faker, di default impostato come line, ovvero genera una riga della stessa lunghezza ma con caratteri casuali)
- linked_to (Specifica che la colonna è collegata ad un’altra, utilizzando quindi la stessa anonimizzazione dell’altra colonna, di default impostato come None, quindi non collegata)

*Esempio:*
<nameschema:"nome_dello_schema"; nametable:"nome_della_tabella"; columnname:"nome_della_colonna"; linked_to:"(nome_dello_schema_con_tabella)nome_della_colonna"; randomtype:"tipologia_di_anonimizzazione">
