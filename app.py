from flask import Flask, request
from crontab import CronTab
from datetime import datetime
from apscheduler.scheduler import Scheduler
from transformation import TribesDataTransporter

app = Flask(__name__)
# Start the scheduler
sched = Scheduler()
sched.start()



# to store the all data to graph database(azure cosmos DB) from google cloud bucket
@app.route("/sync/full_sync", methods=["GET"])
def sync_full_data():
    try:
        full_sync = TribesDataTransporter('full_sync')
        return "Success"
    except:
        return "Failed"


# to store the remaining or new added data in graph database(azure cosmos DB) from google cloud bucket
@app.route("/sync/partial_sync", methods=["GET"])
def sync_partial_data():
    try:
        partial_sync = TribesDataTransporter('partial_sync')
        return "Success"
    except:
        return "Failed"


@app.route('/')
def hello():
    return "Hello World!"

# Add the cron jobs
def add_cron_jobs():
    # cron for partial sync
    partial_sync_cron = { "second": "*", "minute": "*", "hour": "8", "day_of_week": "*", "day": "*", "week": "*", "month": "*", "year":"*"}
    sched.add_cron_job(sync_partial_data, **partial_sync_cron)

if __name__ == '__main__':
    add_cron_jobs()
    app.run()
