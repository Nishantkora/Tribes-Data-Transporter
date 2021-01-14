
Steps to run the app

1. set up the google cloud in your local system and create new virtual environment

2. create project in google cloud and get the credentials and "credentials.json" store it in your installation folder of google cloud

3. Install python and install requirements.txt file from project under root folder
    pip install -r requirements.txt

4. execute the below steps to set environment variables under root of project folder
    a. set GOOGLE_APPLICATION_CREDENTIALS=<path of google cloud credential>
    b. set ENDPOINT=wss://intership-assignments-2020.gremlin.cosmos.azure.com:443/
    c. set USERNAME=/dbs/graph-data-nishant-kora/colls/graph-data-nishant-kora
    d. set PASSWORD=9bpwqQIoaLegfAtUtTJIN8F4bsSPA1bcpqxBVBl0S7eOvRXAp5IdMIPWITj3qGhGVJsLzwk1s4hu3annNsWSag==
    e. set BUCKETNAME=data-engineering-intern-data

5. open app.py set the scheduler so that it runs on specfic time

6. Running the app :- python app.py

7. call the /sync/full_sync ---> to sync allthe files from bucket

8. call the /sync/partial_sync ---> to sync newly ctreated files from bucket

Instructions:

1. storing the last sync date of data is not storing in db...once we do that it will be easier to do the partial sync.
   so currently we are storing last sync date in the program itself