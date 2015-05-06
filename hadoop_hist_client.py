import requests, json

pretty_print = True

def to_json_str(obj):
    if pretty_print:
        return json.dumps(obj, indent=4)
    else:
        return json.dumps(obj)

class JobHistoryServer:

    def __init__(self, host, port=19888):
        self.host = host
        self.port = port
        self.jobs = None
        self.url = "http://" + host + ":" + str(port) + "/ws/v1/history/mapreduce"

    def request(self, uri):
        return requests.get(self.url + uri).json()

    def jobs(self):
        if self.jobs == None:
            self.jobs = Jobs(self)
        return self.jobs

class Jobs:

    def __init__(self, jobHistoryServer):
        self.server = jobHistoryServer
        self.jobs = None
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, index):
        return self.to_job(self.jobs[index])

    def update(self):
        self.raw = self.server.request('/jobs')
        self.jobs = self.raw['jobs']['job']

    def all(self):
        if self.jobs == None:
            self.jobs = [self.to_job(job) for job in self.jobs]
        return self.jobs

    def job_ids(self):
        return [job['id'] for job in self.jobs]

    def find_by(self, key, value):
        for job in self.jobs:
            if job[key] == value:
                return self.to_job(job)

    def filter(self, key, value):
        return [self.to_job(job) for job in self.jobs if job[key] == value]

    def fuzzy_filter(self, key, substring):
        return [self.to_job(job) for job in self.jobs if job[key].find(substring) >= 0]

    def to_job(self, job):
        return Job(self.server, job['id'])

class Job:

    def __init__(self, jobHistoryServer, job_id):
        self.server = jobHistoryServer
        self.job_id = job_id
        self.tasks_list = None
        self.counters = None
        self.update()

    def __getitem__(self, key):
        return self.job[key]

    def __str__(self):
        return to_json_str(self.raw)

    def __repr__(self):
        return self.__str__()

    def update(self):
        self.raw = self.server.request(self.uri())
        self.job = self.raw['job']

    def uri(self):
        return '/jobs/' + self.job_id

    def tasks(self):
        if self.tasks_list == None:
            tasks = self.server.request(self.uri() + '/tasks')['tasks']['task']
            self.tasks_list = [Task(self.server, self.job_id, task['id']) for task in tasks]
        return self.tasks_list

    def counters(self):
        if self.counters == None:
            counters = self.server.request(self.uri() + '/counters')
            self.counters = Counters(counters)
        return self.counters

class Task:

    def __init__(self, jobHistoryServer, job_id, task_id):
        self.server = jobHistoryServer
        self.job_id = job_id
        self.task_id = task_id
        self.attempts_list = None
        self.counters = None
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.task[key]

    def update(self):
        self.raw = self.server.request(self.uri())
        self.task = self.raw['task']

    def uri(self):
        return '/jobs/' + self.job_id + '/tasks/' + self.task_id

    def counters(self):
        if self.counters == None:
            counters = self.server.request(self.uri() + '/counters')
            self.counters = Counters(counters)
        return self.counters

    def attempts(self):
        if self.attempts_list == None:
            attempts = self.server.request(self.uri() + '/attempts')['taskAttempts']['taskAttempt']
            self.attempts_list = [Attempt(self.server, self.job_id, self.task_id, attempt['id']) for attempt in attempts]
        return self.attempts_list

class Attempt:

    def __init__(self, jobHistoryServer, job_id, task_id, attempt_id):
        self.server = jobHistoryServer
        self.job_id = job_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.counters = None
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.attempt[key]

    def update(self):
        self.raw = self.server.request(self.uri())
        self.attempt = self.raw['taskAttempt']

    def uri(self):
        return '/jobs/' + self.job_id + '/tasks/' + self.task_id + '/attempts/' + self.attempt_id

    def counters(self):
        if self.counters == None:
            counters = self.server.request(self.uri() + '/counters')
            self.counters = Counters(counters)
        return self.counters

class Counters:

    def __init__(self, counters_dict):
        self.counters = counters_dict

    def __str__(self):
        return to_json_str(self.counters)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        return self.counters[key]
