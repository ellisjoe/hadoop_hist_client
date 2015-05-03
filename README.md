Hadoop Job History Client
=========================

This client provides a means to consume the APIs exposed by Hadoop's Job History
Server.

Quickstart
----------

    from hadoop_hist_client import JobHistoryServer

    server = JobHistoryServer("hostname")
    jobs = server.jobs()
    filtered_jobs = jobs.filter('name', 'test_job')
    print [job['state'] for job in filtered_jobs]
