from dagster import ScheduleDefinition, DefaultScheduleStatus
from ..jobs import trip_update_job, weekly_update_job
from ..jobs.complex_job import diamond

trip_update_schedule = ScheduleDefinition(
    job=trip_update_job,
    cron_schedule="1/5 * * * *",
    default_status=DefaultScheduleStatus.RUNNING
)


weekly_update_schedule = ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="1/10 * * * *", # # every Monday at midnight
    default_status=DefaultScheduleStatus.RUNNING
    
)


diamond_schedule = ScheduleDefinition(
    job=diamond,
    cron_schedule="* * * * *", # # every Monday at midnight
    default_status=DefaultScheduleStatus.RUNNING
    
)