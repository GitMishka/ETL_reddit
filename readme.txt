extract_hot.py & extract_new.py
    extract posts and misc data every minute from reddit r/all using reddit API 
    put results into postgres db hosted on aws

transform_load.py
    transform data
    move transformed data from original db and load into another db 
