"""
Course    : CSE 351
Assignment: 04
Student   : <Zac Volmer>

Instructions:
    - review instructions in the course

In order to retrieve a weather record from the server, Use the URL:

f'{TOP_API_URL}/record/{name}/{recno}

where:

name: name of the city
recno: record number starting from 0

"""

import time
import threading
import queue

from common import *

from cse351 import *

THREADS = 100               # TODO - set for your program
WORKERS = 10
RECORDS_TO_RETRIEVE = 5000  # Don't change


# ---------------------------------------------------------------------------

def retrieve_weather_data(command_q, work_q):
    while True:
        cmd = command_q.get()
        try:
            if cmd is None:
                break
            city, recno = cmd
            data = get_data_from_server(f'{TOP_API_URL}/record/{city}/{recno}')
            if data is not None:
                work_q.put((city, data['date'], data['temp']))
        finally:
            command_q.task_done()


# ---------------------------------------------------------------------------
# TODO - Create Worker threaded class

class Worker(threading.Thread):
    def __init__(self, work_q, noaa):
        super().__init__()
        self.work_q = work_q
        self.noaa = noaa

    def run(self):
        while True:
            item = self.work_q.get()
            try:
                if item is None:
                    break
                city, date, temp = item
                self.noaa.add_temp(city, date, temp)
            finally:
                self.work_q.task_done()

# ---------------------------------------------------------------------------
# TODO - Complete this class

class NOAA:

    def __init__(self):
        self._data = {name: [] for name in CITIES}
        self._lock = threading.Lock()

    def add_temp(self, city, date, temp):
        with self._lock:
            self._data[city].append(float(temp))

    def get_temp_details(self, city):
        with self._lock:
            temps = self._data.get(city, [])
            if not temps:
                return 0.0
            return sum(temps) / len(temps)



# ---------------------------------------------------------------------------
def verify_noaa_results(noaa):

    answers = {
        'sandiego': 14.5004,
        'philadelphia': 14.865,
        'san_antonio': 14.638,
        'san_jose': 14.5756,
        'new_york': 14.6472,
        'houston': 14.591,
        'dallas': 14.835,
        'chicago': 14.6584,
        'los_angeles': 15.2346,
        'phoenix': 12.4404,
    }

    print()
    print('NOAA Results: Verifying Results')
    print('===================================')
    for name in CITIES:
        answer = answers[name]
        avg = noaa.get_temp_details(name)

        if abs(avg - answer) > 0.00001:
            msg = f'FAILED  Expected {answer}'
        else:
            msg = f'PASSED'
        print(f'{name:>15}: {avg:<10} {msg}')
    print('===================================')


# ---------------------------------------------------------------------------
def main():

    log = Log(show_terminal=True, filename_log='assignment.log')
    log.start_timer()

    noaa = NOAA()

    # Start server
    data = get_data_from_server(f'{TOP_API_URL}/start')

    # Get all cities number of records
    print('Retrieving city details')
    city_details = {}
    name = 'City'
    print(f'{name:>15}: Records')
    print('===================================')
    for name in CITIES:
        city_details[name] = get_data_from_server(f'{TOP_API_URL}/city/{name}')
        print(f'{name:>15}: Records = {city_details[name]["records"]:,}')
    print('===================================')

    records = RECORDS_TO_RETRIEVE

    # TODO - Create any queues, pipes, locks, barriers you need

    command_q = queue.Queue(maxsize=10)
    work_q = queue.Queue(maxsize=10)

    workers = []
    for _ in range(WORKERS):
        w = Worker(work_q, noaa)
        w.start()
        workers.append(w)

    retrievers = []
    for _ in range(THREADS):
        t = threading.Thread(target=retrieve_weather_data, args=(command_q, work_q))
        t.start()
        retrievers.append(t)

    for city in CITIES:
        for recno in range(records):
            command_q.put((city, recno))

    for _ in range(THREADS):
        command_q.put(None)

    command_q.join()

    for t in retrievers:
        t.join()

    for _ in range(WORKERS):
        work_q.put(None)

    work_q.join()

    for w in workers:
        w.join()

    # End server - don't change below
    data = get_data_from_server(f'{TOP_API_URL}/end')
    print(data)

    verify_noaa_results(noaa)

    log.stop_timer('Run time: ')


if __name__ == '__main__':
    main()
