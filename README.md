# BulkDNS

Application/System for bulk check of available domains.


##  Functionality and features

Main modules (some are still under development - see roadmap):

* Database structure initialization and all possible short domain names generator (up to 5 chars)
* Core domain check processing (single processing, multiprocessing & multithreading support)
* Archiving of taken domains & bringing back expiring domains to execute re-check
* Simple UI (web instance + web interface)
* Custom domain names dictionary upload and check functionality
* SQLite and PostgreSQL DB supported


##  Installation and run

* install whois binary 
  * For Windows: 
    * Download from https://learn.microsoft.com/en-us/sysinternals/downloads/whois
    * Add PATH location of your whois.exe to Environment System Variables
  * For Linux (Debian family):
    ```
    sudo apt update
    sudo apt upgrade
    sudo apt install whois
    ``` 
* install dependencies (see below)
* review and modify ```config.cfg``` located in root diectory
* open cmd terminal or shell & run application: ```python main.py```


## Dependencies

Before installing python libraries upgrade pip: ```pip install --upgrade pip```

* Python standard library: 3.12 (https://www.python.org/)

* PostgreSQL database adapter for Python: Psycopg 3 (https://www.psycopg.org/)

  ```pip install "psycopg[binary,pool]"```

* WhoisDomain package by mboot (https://github.com/mboot-github/WhoisDomain/)

  ```pip install whoisdomain```


## ChangeLog

[ChangeLog](doc/CHANGELOG.md)


## Bugs & To-Do list

[Bugs & To-Do](doc/BUGS&TODO.md)


## Roadmap

[Roadmap](doc/ROADMAP.md)


## Authors and contributors

[Authors](doc/AUTHORS.md)
