import json
from datetime import datetime, timedelta, date
# from airflow.decorators import dag, task
# from airflow.models import Variable
# from utils.helpers import job_success_message, notify_slack
# from utils.config import default_dag_args
# from utils.connections import get_mysql_conn

DATABASE = "BLAZE_PROD_APP_DB"
DAILY_DATA_MONITORING_SLACK_URL = Variable.get("SLACK_DAILY_DATA_USAGE_WEBHOOK_URL")
WEEKLY_DATA_MONITORING_SLACK_URL = Variable.get("SLACK_WEEKLY_DATA_USAGE_WEBHOOK_URL")


def get_total_emails_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'emailsSent' THEN 1 ELSE 0 END) AS Sent
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(Sent) AS total_emails_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_replies(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'emailsReplied' THEN 1 ELSE 0 END) AS Replied
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(Replied) AS total_replies
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_unique_leads(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                COUNT(DISTINCT CASE WHEN TYPE = 'emailsSent' AND CREATEDAT = fe.FirstEmailSent THEN EMAIL ELSE NULL END) AS NewLeads
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities wd
            LEFT JOIN (
                SELECT EMAIL, MIN(CREATEDAT) AS FirstEmailSent
                FROM blaze_leadgen.public.lemlist_campaign_activities
                WHERE TYPE = 'emailsSent'
                GROUP BY EMAIL
            ) fe ON wd.EMAIL = fe.EMAIL
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(NewLeads) AS total_unique_leads
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_li_requests_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'linkedinSent' THEN 1 ELSE 0 END) AS RequestsSent
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(RequestsSent) AS total_li_requests_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_li_messages_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'linkedinInviteDone' THEN 1 ELSE 0 END) AS InviteDone
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(InviteDone) AS total_li_messages_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_unique_x_dms(cursor):
    cursor.execute("""
        WITH messages_sent AS (
            SELECT 
                lead_id, 
                DATEADD(DAY, 6 - DAYOFWEEK(TO_DATE(created_at)), TO_DATE(created_at)) AS week_ending
            FROM blaze_staging_app_db.mysql.sequence_actions_history 
            WHERE sequence_id IN (
                SELECT uuid 
                FROM blaze_staging_app_db.mysql.sequences 
                WHERE twitter_oauth_id IN (411, 522) 
                  AND UPDATED_DATE = current_date()
            )  
            AND UPDATED_DATE = current_date() 
            AND ACTION_TYPE = 'SEND_TWITTER_DM' 
            AND STATUS = 'SUCCESS'
        )
        SELECT COUNT(DISTINCT lead_id) AS total_unique_x_dms
        FROM messages_sent
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_total_x_responses(cursor):
    cursor.execute("""
        WITH replies_received AS (
            SELECT 
                lead_id, 
                DATEADD(DAY, 6 - DAYOFWEEK(TO_DATE(created_at)), TO_DATE(created_at)) AS week_ending
            FROM blaze_staging_app_db.mysql.sequence_actions_history 
            WHERE sequence_id IN (
                SELECT uuid 
                FROM blaze_staging_app_db.mysql.sequences 
                WHERE twitter_oauth_id IN (411, 522) 
                  AND UPDATED_DATE = current_date() 
            ) 
            AND trigger_type = 'REPLIED' 
            AND UPDATED_DATE = current_date() 
        )
        SELECT COUNT(DISTINCT lead_id) AS total_x_responses
        FROM replies_received
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_emails_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'emailsSent' THEN 1 ELSE 0 END) AS Sent
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(Sent) AS weekly_emails_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_new_leads(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                COUNT(DISTINCT CASE WHEN TYPE = 'emailsSent' AND CREATEDAT = fe.FirstEmailSent THEN EMAIL ELSE NULL END) AS NewLeads
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities wd
            LEFT JOIN (
                SELECT EMAIL, MIN(CREATEDAT) AS FirstEmailSent
                FROM blaze_leadgen.public.lemlist_campaign_activities
                WHERE TYPE = 'emailsSent'
                GROUP BY EMAIL
            ) fe ON wd.EMAIL = fe.EMAIL
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(NewLeads) AS weekly_new_leads
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_replies(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'emailsReplied' THEN 1 ELSE 0 END) AS Replied
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(Replied) AS weekly_replies
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_li_requests_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'linkedinSent' THEN 1 ELSE 0 END) AS RequestsSent
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(RequestsSent) AS weekly_li_requests_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_li_messages_sent(cursor):
    cursor.execute("""
        WITH weekly_data AS (
            SELECT
                DATEADD(DAY, 6 - EXTRACT(DOW FROM TO_TIMESTAMP(CREATEDAT)), TO_TIMESTAMP(CREATEDAT)) AS week_ending,
                SUM(CASE WHEN TYPE = 'linkedinInviteDone' THEN 1 ELSE 0 END) AS InviteDone
            FROM 
                blaze_leadgen.public.lemlist_campaign_activities
            WHERE 
                CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', TO_TIMESTAMP(CREATEDAT)) >= DATEADD(WEEK, -1, CURRENT_DATE)
                AND (CAMPAIGNNAME LIKE '%Web3%' OR CAMPAIGNNAME LIKE '%Web2%')
            GROUP BY week_ending
        )
        SELECT SUM(InviteDone) AS weekly_li_messages_sent
        FROM weekly_data
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_unique_x_dms(cursor):
    cursor.execute("""
        WITH messages_sent AS (
            SELECT 
                lead_id, 
                DATEADD(DAY, 6 - DAYOFWEEK(TO_DATE(created_at)), TO_DATE(created_at)) AS week_ending
            FROM blaze_staging_app_db.mysql.sequence_actions_history 
            WHERE sequence_id IN (
                SELECT uuid 
                FROM blaze_staging_app_db.mysql.sequences 
                WHERE twitter_oauth_id IN (411, 522) 
                  AND UPDATED_DATE = current_date()
            )  
            AND UPDATED_DATE = current_date() 
            AND ACTION_TYPE = 'SEND_TWITTER_DM' 
            AND STATUS = 'SUCCESS'
        )
        SELECT COUNT(DISTINCT lead_id) AS weekly_unique_x_dms
        FROM messages_sent
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0

def get_weekly_x_responses(cursor):
    cursor.execute("""
        WITH replies_received AS (
            SELECT 
                lead_id, 
                DATEADD(DAY, 6 - DAYOFWEEK(TO_DATE(created_at)), TO_DATE(created_at)) AS week_ending
            FROM blaze_staging_app_db.mysql.sequence_actions_history 
            WHERE sequence_id IN (
                SELECT uuid 
                FROM blaze_staging_app_db.mysql.sequences 
                WHERE twitter_oauth_id IN (411, 522) 
                  AND UPDATED_DATE = current_date() 
            ) 
            AND trigger_type = 'REPLIED' 
            AND UPDATED_DATE = current_date() 
        )
        SELECT COUNT(DISTINCT lead_id) AS weekly_x_responses
        FROM replies_received
        WHERE week_ending = DATEADD(DAY, 6 - EXTRACT(DOW FROM CURRENT_DATE()), CURRENT_DATE());
    """)
    result = cursor.fetchone()
    return result[0] if result else 0




@dag(
    description="Daily and Weekly Slack Alerts for Data Usage Monitoring",
    schedule_interval="*/5 0 * * *",  # Run every 5 minutes at 00:00 UTC
    start_date=datetime(2024, 8, 13),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_dag_args,
)
def data_usage_monitoring_dag():
    @task()
    def daily_data_usage_monitoring():
        try:
            # Connection to our main SQL database
            mysql_conn = get_mysql_conn()
            mysql_cur = mysql_conn.cursor()
            
            total_emails_sent = get_total_emails_sent(mysql_cur)
            total_replies = get_total_replies(mysql_cur)
            total_unique_leads = get_total_unique_leads(mysql_cur)
            total_li_requests_sent = get_total_li_requests_sent(mysql_cur)
            total_li_messages_sent = get_total_li_messages_sent(mysql_cur)

            total_unique_x_dms = get_total_unique_x_dms(mysql_cur)
            total_x_responses = get_total_x_responses(mysql_cur)

            notify_slack(
                f"""
                🚨 Daily Data Usage Monitoring Alert!\n\n📋 Metrics:\n\nLemList:\n- Total Emails Sent: {total_emails_sent}\n- Total Replies: {total_replies}\n- Total Unique Leads: {total_unique_leads}\n- Total LI Requests Sent: {total_li_requests_sent}\n- Total LI Messages Sent: {total_li_messages_sent}\n\nX (Lauren+Chirag):\n- Total Unique X DMs: {total_unique_x_dms}\n- Total X Responses: {total_x_responses}\n🎯 
                """,
                DAILY_DATA_MONITORING_SLACK_URL,
            )
            job_success_message()
        except Exception as e:
            msg = "Error {} in fetching data".format(e)
            print(msg)
        finally:
            mysql_conn.close()

    @task()
    def weekly_data_usage_monitoring():
        try:
            # Connection to our main SQL database
            mysql_conn = get_mysql_conn()
            mysql_cur = mysql_conn.cursor()

            # Get data for Lemlist
            weekly_emails_sent = get_weekly_emails_sent(mysql_cur)
            weekly_new_leads = get_weekly_new_leads(mysql_cur)
            weekly_replies = get_weekly_replies(mysql_cur)
            weekly_li_requests_sent = get_weekly_li_requests_sent(mysql_cur)
            weekly_li_messages_sent = get_weekly_li_messages_sent(mysql_cur)

            # Get data for X (Lauren+Chirag)
            weekly_unique_x_dms = get_weekly_unique_x_dms(mysql_cur)
            weekly_x_responses = get_weekly_x_responses(mysql_cur)

            notify_slack(
                f"""
                🚨 Weekly Data Usage Monitoring Alert!\n\n📋 Metrics:\n\nLemList:\n- Weekly Emails Sent: {weekly_emails_sent}\n- Weekly New Leads: {weekly_new_leads}\n- Weekly Replies: {weekly_replies}\n- Weekly LI Requests Sent: {weekly_li_requests_sent}\n- Weekly LI Messages Sent: {weekly_li_messages_sent}\n\nX (Lauren+Chirag):\n- Weekly Unique X DMs: {weekly_unique_x_dms}\n- Weekly X Responses: {weekly_x_responses}\n🎯 
                """,
                WEEKLY_DATA_MONITORING_SLACK_URL,
            )
            job_success_message()
        except Exception as e:
            msg = "Error {} in fetching data".format(e)
            print(msg)
        finally:
            mysql_conn.close()

    daily_data_usage_monitoring()
    if date.today().weekday() == 0:  # Monday
        weekly_data_usage_monitoring()

data_usage_monitoring_dag_obj = data_usage_monitoring_dag()