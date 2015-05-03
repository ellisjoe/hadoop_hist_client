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
        self.url = "http://" + host + ":" + str(port) + "/ws/v1/history/mapreduce"

    def request(self, uri):
        return requests.get(self.url + uri).json()

    def jobs(self):
        return Jobs(self)

class Jobs:

    def __init__(self, jobHistoryServer):
        self.server = jobHistoryServer
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
        return [self.to_job(job) for job in self.jobs]

    def job_ids(self):
        return [job['id'] for job in self.jobs]

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
        tasks = self.server.request(self.uri() + '/tasks')['tasks']['task']
        return [Task(self.server, self.job_id, task['id']) for task in tasks]

    def counters(self):
        counters = self.server.request(self.uri() + '/counters')
        return Counters(counters)

class Task:

    def __init__(self, jobHistoryServer, job_id, task_id):
        self.server = jobHistoryServer
        self.job_id = job_id
        self.task_id = task_id
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
        counters = self.server.request(self.uri() + '/counters')
        return Counters(counters)

    def attempts(self):
        attempts = self.server.request(self.uri() + '/attempts')['taskAttempts']['taskAttempt']
        return [Attempt(self.server, self.job_id, self.task_id, attempt['id']) for attempt in attempts]

class Attempt:

    def __init__(self, jobHistoryServer, job_id, task_id, attempt_id):
        self.server = jobHistoryServer
        self.job_id = job_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def __repr__(self):
        return self.__str__()

    def update(self):
        self.raw = self.server.request(self.uri())
        self.attempt = self.raw['taskAttempt']

    def uri(self):
        return '/jobs/' + self.job_id + '/tasks/' + self.task_id + '/attempts/' + self.attempt_id

    def counters(self):
        counters = self.server.request(self.uri() + '/counters')
        return Counters(counters)

class Counters:

    def __init__(self, counters_dict):
        self.counters = counters_dict

    def __str__(self):
        return to_json_str(self.counters)

    def __repr__(self):
        self.__str__()

    def __getitem__(self, key):
        return self.counters[key]
