# Bugs & Fixes, To-Do 

## Bugs

* Very high memory consumption (>16 GB) for five_digit_letter names generator.
Problem is not during names itself generation, but at stage of SQL insert data preparation. Possible fix to change the logic of create_domain_dta() to execute insert every 1-2M records during data preparation, not afterwards. 


## To-Do list

* Before whois processing starts add verification if whois binary is installed, otherwise application will try to process checks (not harmful as result of check will raise error '-1' and no update on each checked domain will be done). Still whole processing is unneeded and there is no indication that something is wrong)

## Closed

