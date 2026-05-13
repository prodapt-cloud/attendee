import logging
import os
import signal
import subprocess
import time

from celery import shared_task
from celery.signals import worker_shutting_down
from celery.exceptions import SoftTimeLimitExceeded

from bots.bot_controller import BotController
from bots.models import Bot, BotEventManager, BotEventSubTypes, BotEventTypes

logger = logging.getLogger(__name__)


RUN_BOT_SOFT_TIME_LIMIT_SECONDS = int(os.getenv("RUN_BOT_SOFT_TIME_LIMIT_SECONDS", 3600))
RUN_BOT_HARD_TIME_LIMIT_SECONDS = int(os.getenv("RUN_BOT_HARD_TIME_LIMIT_SECONDS", 3660))


@shared_task(bind=True, soft_time_limit=RUN_BOT_SOFT_TIME_LIMIT_SECONDS, time_limit=RUN_BOT_HARD_TIME_LIMIT_SECONDS)
def run_bot(self, bot_id):
    logger.info(f"Running bot {bot_id}")
    try:
        bot_controller = BotController(bot_id)
        bot_controller.run()
    except SoftTimeLimitExceeded:
        logger.exception("run_bot soft time limit exceeded for bot %s; marking bot fatal and terminating child browser processes", bot_id)
        try:
            bot = Bot.objects.get(id=bot_id)
            if BotEventManager.event_can_be_created_for_state(BotEventTypes.FATAL_ERROR, bot.state):
                BotEventManager.create_event(
                    bot=bot,
                    event_type=BotEventTypes.FATAL_ERROR,
                    event_sub_type=BotEventSubTypes.FATAL_ERROR_PROCESS_TERMINATED,
                    event_metadata={
                        "reason": "run_bot_soft_time_limit_exceeded",
                        "soft_time_limit_seconds": RUN_BOT_SOFT_TIME_LIMIT_SECONDS,
                        "hard_time_limit_seconds": RUN_BOT_HARD_TIME_LIMIT_SECONDS,
                    },
                )
        except Exception:
            logger.exception("Failed to mark bot %s fatal after run_bot soft time limit", bot_id)

        kill_child_process_tree(os.getpid())
        raise


def kill_child_process_tree(pid):
    try:
        child_pids = subprocess.check_output(["pgrep", "-P", str(pid)], text=True).split()
    except subprocess.CalledProcessError:
        child_pids = []
    except Exception:
        logger.exception("Failed to list child processes for pid %s", pid)
        child_pids = []

    for child_pid in child_pids:
        child_pid = int(child_pid)
        kill_child_process_tree(child_pid)
        for sig in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.kill(child_pid, sig)
                if sig == signal.SIGTERM:
                    time.sleep(0.2)
            except ProcessLookupError:
                break
            except Exception:
                logger.exception("Failed to send signal %s to child pid %s", sig, child_pid)
                break


def kill_child_processes():
    # Get the process group ID (PGID) of the current process
    pgid = os.getpgid(os.getpid())

    try:
        # Send SIGTERM to all processes in the process group
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass  # Process group may no longer exist


@worker_shutting_down.connect
def shutting_down_handler(sig, how, exitcode, **kwargs):
    # Just adding this code so we can see how to shut down all the tasks
    # when the main process is terminated.
    # It's likely overkill.
    logger.info("Celery worker shutting down, sending SIGTERM to all child processes")
    kill_child_processes()
