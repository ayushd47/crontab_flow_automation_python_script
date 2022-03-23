# TIKtvg59710
#import schedule
import time
#import logging
import tableauserverclient as TSC
import polling2
import time
import logging
# add your server link below
server = TSC.Server(
    "https://prod-apnortheast-a.online.tableau.com/",  use_server_version=True)
# add your access tokenauth below
tableau_auth = TSC.PersonalAccessTokenAuth(
    "", "",  site_id="")

logger = logging.getLogger('Schedule')
logger.setLevel(logging.DEBUG)
# create file handler that logs debug and higher level messages
fh = logging.FileHandler('/var/log/tableau-cron.log', 'a', 'utf-8')
fh.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
# create formatter and add it to the handlers
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add the handlers to logger
logger.addHandler(ch)
logger.addHandler(fh)


def job():
    try:
        with server.auth.sign_in(tableau_auth):
            all_flow_items, pagination_item = server.flows.get()
            for flow in all_flow_items:
                # add your flow project flow name below
                if(flow.project_name == ""):
                    logger.debug(f"Running flow :: {flow.name}, {flow.id}")
                    try:
                        job = server.flows.refresh(flow)
                        job_status = ["Success", "Failed", "Cancelled"]
                        polling2.poll(lambda: server.jobs.get_by_id(
                            job.id).finish_code != -1, step=30, poll_forever=True)
                        logger.debug('Job finished with status: ' +
                                     job_status[int(server.jobs.get_by_id(job.id).finish_code)])
                    except Exception as e:
                        logger.error(e)
    except Exception as e:
        logger.error(e)


job()

# schedule.every().day.at("09:53").do(job)

# while True:
# schedule.run_pending()
# time.sleep(1)
