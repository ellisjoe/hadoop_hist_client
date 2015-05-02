import requests, json

host = "node-1.hadoop-slow.ucare.emulab.net"
port = "19888"
pretty_print = True

def jobHistoryRequest(uri):
    url = "http://" + host + ":" + port + "/ws/v1/history/mapreduce"
    r = requests.get(url + uri)
    return r.json()

def to_json_str(obj):
    if pretty_print:
        return json.dumps(obj, indent=4)
    else:
        return json.dumps(obj)

class Jobs:

    def __init__(self):
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def __getitem__(self, index):
        return self.jobs[index]

    def update(self):
        self.raw = jobHistoryRequest('/jobs')
        jobs_dict = self.raw['jobs']['job']
        self.jobs = [Job(job['id']) for job in jobs_dict]

    def all(self):
        return self.jobs

    def job_ids(self):
        return [job['id'] for job in self.jobs]

class Job:

    def __init__(self, job_id):
        self.job_id = job_id
        self.update()

    def __getitem__(self, key):
        return self.job[key]

    def __str__(self):
        return to_json_str(self.raw)

    def update(self):
        self.raw = jobHistoryRequest(self.uri())
        self.job = self.raw['job']

    def uri(self):
        return '/jobs/' + self.job_id

    def tasks(self):
        tasks = jobHistoryRequest(self.uri() + '/tasks')['tasks']['task']
        return [Task(self.job_id, task['id']) for task in tasks]

    def counters(self):
        counters = jobHistoryRequest(self.uri() + '/counters')
        return Counters(counters)

class Task:

    def __init__(self, job_id, task_id):
        self.job_id = job_id
        self.task_id = task_id
        self.update()

    def __str__(self):
        to_json_str(self.raw)

    def update(self):
        self.raw = jobHistoryRequest(self.uri())
        self.task = self.raw['task']

    def uri(self):
        return '/jobs/' + self.job_id + '/tasks/' + self.task_id

    def counters(self):
        counters = jobHistoryRequest(self.uri() + '/counters')
        return Counters(counters)

    def attempts(self):
        attempts = jobHistoryRequest(self.uri() + '/attempts')['taskAttempts']['taskAttempt']
        return [Attempt(self.job_id, self.task_id, attempt['id']) for attempt in attempts]

class Attempt:

    def __init__(self, job_id, task_id, attempt_id):
        self.job_id = job_id
        self.task_id = task_id
        self.attempt_id = attempt_id
        self.update()

    def __str__(self):
        return to_json_str(self.raw)

    def update(self):
        self.raw = jobHistoryRequest(self.uri())
        self.attempt = self.raw['taskAttempt']

    def uri(self):
        return '/jobs/' + self.job_id + '/tasks/' + self.task_id + '/attempts/' + self.attempt_id

    def counters(self):
        counters = jobHistoryRequest(self.uri() + '/counters')
        return Counters(counters)

class Counters:

    def __init__(self, counters_dict):
        self.counters = counters_dict

    def __str__(self):
        return to_json_str(self.counters)

    def __getitem__(self, key):
        return self.counters[key]
