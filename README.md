### What is This?

This is a Python process that queries a db every minute and runs SQL or Python if the job is scheduled to run for that minute. Jobs are created via the [R2D4](https://dataline.mskcc.org/DataLine) web application by using the Scheduler tab. 'Email Attachment' jobs extract data to Excel and send via email. 'Python' jobs simply run a script and don't necessarily send email unless email sending is programmed in the respective script to be run.

### Architecture

Host: vsmsktdvbi1

Location: C:\DataLine\r2d4_scheduler

Database that stores scheduled jobs: PS23A:61692

Database Tables:
- dbo.SCHEDULER - is the main table that has job project code (i.e. IS15906)
- dbo.SCHEDULER_QUEUE - will temporarily hold any job that has a prerequisite (i.e. needs to wait for ETL) until it is valid to run it
- dbo.SCHEDULER_RUN_NOW - will temporarily hold any job that you want to run now (it will run at the next minute since the scheduler runs every minute)
- dbo.SCHEDULER_RECIPIENTS - networkids of people and PDLs who receive a particular job
- dbo.SCHEDULER_LOG - start and end time of previously run jobs and also RUN_NOTES (i.e. error messages if job failed)
- dbo.SCHEDULER_DELIVERY_TYPES - lookup table of types available for job (i.e. Email Attachment, Python Script)
- dbo.SCHEDULER_DAYS_OF_MONTH - table only needed if job should run on certain days of month (i.e. 1st and 15th) instead of days of week

### Processes

- On the host vsmsktdvbi1, Windows scheduler runs C:\DataLine\r2d4_scheduler\r2d4_scheduler.py every minute
- On the host vsmsktdvbi1, Windows scheduler runs C:\DataLine\r2d4_scheduler\r2d4_scheduler_cleanup.py once a week (every Wednesday at 4:30 pm) to remove extracted Excel files older than 14 days

### How To

- **Suspend everything** - either stop Windows scheduler on vsmsktdvbi1 or run SQL against the table dbo.SCHEDULER to set the [Enabled] field to 0
- **Check if a job ran and how long it took** - using R2D4 you can check the Scheduler tab -> Run Log section. Time is measured in seconds. The same data also exists in the dbo.SCHEDULER_LOG table in PS23A:61692