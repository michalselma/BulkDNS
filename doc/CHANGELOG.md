# CHANGELOG

## 0.9
* Introduction of dictionary check functionality
* Fix of poor used memory clean-up in multi-process and rdap subprocess components
* Data consistency & validation checks added to archiving process

## 0.8.2
* init, archive and proc modules modified, as part of preapration for dictionary functionality introduction
* configuration file rebuild
* re-check functionality for available domains


## 0.8.1
* domain module improvements and rebuild


## 0.8
* RDAP functionality introduction
* WHOIS module rebuild, minor cosmetic changes to other modules


## 0.7.2
* Fix of retry function in postgresql module, causing exceptional break of multithread processing, due to UnboundLocalError: cannot access local variable where it is not associated with a value. 
* Minor cosmetic changes in core modules


## 0.7.1
* init_core, proc_core and arch_core fixed to properly match db_type var for postgresql
* default config updated to use 'postgresql' identifier instead of 'postgres'


## 0.7
* multithreading implementation for domains check functionality


## 0.6
* multiprocessing implementation for domains check functionality


## 0.5.1
* fixes & adjustments in single processing module and proc_core


## 0.5
* archiving and backup functionality
* fixes to postgresql module and processing/core module 


## 0.4
* check domains functionality added - single processing within core module
* minor or cosmetic adjustments to postgresql, logger, init_core and main modules


## 0.3.2
* logging fixes & adjustments
* propagate updated logging code to main, init & common modules


## 0.3.1
* instructions, readme & docs update


## 0.3
* logger module introduction
* code adjustments to utilize log functionality


## 0.2
* database structure initialization module
* common libraries module (DB SQL modules)
* configuration


## 0.1 - Initial Release
* project structure - files & folders 
* docs, license, info etc.

