# ETL Data pipeline practice with Python and MongoDB

## Problem Statement
> You are given dirty files that contain patient visit data from specific APIs. Each file contains one patient visit with a set of fields that describe the visit. The fields are:
> - ID: a unique identifier for the visit
> - NAME: the name of the patient
> - AGE: the age of the patient
> - INSTITUTION: the name of the institution where the visit took place
> - ACTIVITY: the type of activity that took place during the visit
> - COMMENT: a comment about the visit
> - GENERATION_CODE: a unique identifier for the file that the visit came from
>
> Your task is to create a data pipeline that will ingest these files and store the data in a database. The database should be able to handle a large number of files
> and visits. The pipeline should be able to be run on a regular basis to ingest new files. The pipeline should be able to be run on a regular basis to ingest new files.

## Test enviroment
> Docker Desktop 4.16.2 (Mac M1)

## Tech Stacks
- Strategy: Create a simple, modular data pipeline that can be easily integrated with other tools.
    * Proof of Concept to comprehend the practical data flow and allow for concise future updates
    * Accelerate development time

- Python:
    * Utilize Python for a rapid and efficient creation of a data pipeline or batch process
    * Robust libraries for ETL processes
- MongoDB:
	* Data stored in JSON format for seamless compatibility with Python, a language familiar to data professionals
	* Scalability: Data is assumed to be continuously retained, necessitating considerations for scaling out

## Sample Data in MongoDB
Each user(patient) visit will be stored in the HISTORY field, maintaining a record of all historical data.

```
{
  "_id": {
    "$oid": "63e9a2a0d74f8026e98e2612"
  },
  "ID": "e38574ba-505b-11ec-aa21-acde48001122",
  "NAME": "user-e38574ba-505b-11ec-aa21-acde48001122",
  "HISTORY": [
    {
      "GENERATION_CODE": "1638111676805696",
      "AGE": "49",
      "INSTITUTION": "University Hospital",
      "ACTIVITY": "outpatient surgery",
      "COMMENT": "something comment"
    }
  ]
}
```


## How to run with local environment setup
### 1. Git clone target repository
```
> git clone git@github.com:lunit-io/di-interview-kjunghan.git
> cd di-interview-kjunghan
```
Directory structure
```
├── INTRODUCTION.md
├── README.md
├── batch
│   ├── data-host
│   └── dockerfile
├── docker-compose.yaml
├── mongodb
│   └── mongo-init.js
```
### 2. run docker compose
```
> docker compose up --build -d
> docker compose ps
NAME                IMAGE                                COMMAND                  SERVICE             CREATED             STATUS              PORTS
mongodb             mongo:6.0.4                          "docker-entrypoint.s…"   mongodb             4 hours ago         Up 4 hours          0.0.0.0:27017->27017/tcp
python-batch        di-interview-kjunghan-python-batch   "python3"                python-batch        4 hours ago         Up 4 hours          0.0.0.0:9000->9000/tcp
```

### 3. Run python batch for starting ETL
```
> docker compose exec python-batch python main.py
```

Check logs
```
> docker compose exec python-batch cat logfile.txt
```

### 4. Confirm MongoDB for checking if all data are loaded
```
> docker compose exec mongodb bash
> mongosh -u deteam -p 1234 accessment
```
or you can use MongoDB GUI tool like [MongoDB Compass](https://www.mongodb.com/products/compass)
> URI for connection: `mongodb://deteam:1234@localhost:27017/?authMechanism=DEFAULT&authSource=accessment`

### Unit test
```
> docker compose exec python-batch python -m unittest test.py
```
