#!/usr/bin/env python3
import argparse
import datetime
import logging
import os
import subprocess
import sys
import time


LOGFILE_PERIOD = datetime.timedelta(minutes=30)
PROFILER_OUTPUT_WAIT_TIMEOUT = datetime.timedelta(seconds=30)
PROFILER_FINISH_INPUT = b'\n' # Input to send a running profiler process to tell it to stop profiling and record the
                              # results.

SIGTERM_GRACE_PERIOD = datetime.timedelta(seconds=5)
SIGNAL_SUCCESS_CHECK_POLL_FREQUENCY = datetime.timedelta(seconds=0.1)


def end_process(process):
    """
    Gracefully terminate a process by sending SIGTERM, waiting up to SIGTERM_GRACE_PERIOD for it to gracefully exit, and
    then sending SIGKILL if process was unable to exit within that grace period.
    """
    sigterm_time = time.time()
    process.terminate()
    while time.time() < sigterm_time + SIGTERM_GRACE_PERIOD - SIGNAL_SUCCESS_CHECK_POLL_FREQUENCY:
        if process.poll() == None:
            # Process exited gracefully
            return
        time.sleep(SIGNAL_SUCCESS_CHECK_POLL_FREQUENCY.total_seconds())
    # Process did not exit gracefully within the grace period, send SIGKILL instead.
    process.kill()
    while process.poll() != None:
        time.sleep(SIGNAL_SUCCESS_CHECK_POLL_FREQUENCY.total_seconds())


def format_datetime_for_filename(value):
    """
    Apply subjective formatting rules to render a datetime as a string compatible with use in a filename.
    """
    formatted_value = value.replace(microsecond=0) # Shrink output by removing unnecessary precision
    formatted_value = formatted_value.isoformat() # Format as ISO8601
    formatted_value = formatted_value.replace('+00:00', 'Z') # Shrink output by using the alternative ISO8601 way of
                                                             # specifying UTC timezone
    formatted_value = formatted_value.replace(':','-') # Remove filename-incompatible special character
    return formatted_value


def get_time_until_next_interval_start(current_datetime, interval_period):
    """
    Given a current time and an interval schedule defined implicitly by intervals starting at midnight and repeating
    every interval_period, return the amount of time between the current time and the start of the next interval
    according to that implicit schedule.
    """
    # Interval period must fit within 24 hours and interval period must evenly divide within 24 hours, such that a
    # period can always start at midnight.
    assert(interval_period <= datetime.timedelta(hours=24))
    assert((datetime.timedelta(hours=24) % interval_period).total_seconds() == 0)

    current_time = current_datetime.time()
    current_time_as_dt = datetime.timedelta(
        seconds=current_time.hour*60*60 + current_time.minute*60 + current_time.second,
        microseconds=current_time.microsecond
    )
    return interval_period - (current_time_as_dt % interval_period)


def run_profiler(run_length, fb_project, extra_profiler_args=None):
    if not extra_profiler_args:
        extra_profiler_args = []
    process = subprocess.Popen(
        [
            'firebase',
            'database:profile',
            '--duration',
            str(run_length.total_seconds()),
            '--project',
            fb_project,
        ] + extra_profiler_args,
        stdin=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    try:
        process.wait(timeout=run_length.total_seconds()+PROFILER_OUTPUT_WAIT_TIMEOUT.total_seconds())
    except subprocess.TimeoutExpired:
        pass # expected behavior
    if process.poll() != None:
        # Process has terminated early, unexpected.
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f'Profiler process terminated early.\n stdout: {stdout.decode("UTF-8")}\n stderr: {stderr.decode("UTF-8")}'
        )
    try:
        stdout, stderr = process.communicate(input=PROFILER_FINISH_INPUT, timeout=PROFILER_OUTPUT_WAIT_TIMEOUT.total_seconds())
    except subprocess.TimeoutExpired:
        end_process(process)
        stdout, stderr = process.communicate()
        raise RuntimeError(
            f'Profiler process failed to terminate.\n stdout: {stdout.decode("UTF-8")}\n stderr: {stderr.decode("UTF-8")}'
        )

    stdout = stdout.decode('UTF-8')

    return stdout


def main():
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s] %(levelname)s - %(message)s')

    script_name = os.path.os.path.splitext(os.path.basename(sys.argv[0]))[0]

    parser = argparse.ArgumentParser()
    default_output_folder = os.path.expanduser(f'~/{script_name}_logs/')
    parser.add_argument(
        '--output-folder', dest='output_folder', default=default_output_folder, help=f'default: {default_output_folder}'
    )
    parser.add_argument(
        '--project', dest='fb_project', help='Name of firebase project to profile.'
    )
    parser.add_argument(
        '--raw', dest='output_format_raw', action='store_true',
        help='Output the raw JSON profiling event data instead of aggregated profiling data.'
    )
    args = parser.parse_args()

    extra_profiler_args = []
    if args.output_format_raw:
        extra_profiler_args.append('--raw')
        output_file_extension = 'json'
    else:
        output_file_extension = 'txt'

    os.makedirs(args.output_folder, exist_ok=True)

    while True:
        current_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        run_length = get_time_until_next_interval_start(current_time, LOGFILE_PERIOD)
        run_start_time = current_time

        start_time_rendered = run_start_time.replace(microsecond=0).isoformat()
        end_time_rendered = (run_start_time + run_length).replace(microsecond=0).isoformat()
        logging.info(f'Starting profile for {start_time_rendered} - {end_time_rendered}')

        try:
            data = run_profiler(run_length, args.fb_project, extra_profiler_args=extra_profiler_args)
        except Exception as e:
            # Recover and keep running despite any errors
            logging.error(str(e))
            continue

        output_filename = f'{args.fb_project}-{format_datetime_for_filename(run_start_time)}.{output_file_extension}'
        output_filepath = os.path.join(args.output_folder, output_filename)
        with open(output_filepath, 'w') as file:
            file.write(data)
        logging.info(f'Wrote out {output_filepath}')


if __name__ == '__main__':
    main()
