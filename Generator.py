import numpy as np
import random
import operator

#Job Class: Each job uses an id number to identify itself and has a size i.e. number tasks to complete
#amount_processed is used to track how much of a job has been finished for memory copy cost
class Job:
    def __init__(self, id, size):
        self.id = id
        self.size = size
        self.amount_processed = 0
    
    #Converts job to string
    def __str__(self):
        return "ID: " + str(self.id) + ", Size: " + str(self.size) + ", Processed: " + str(self.amount_processed)

#Worker Class: Workers keep track of which jobs are currently running and how long they have to complete
#The time it takes a worker to complete is randomly generator between two given values
class Worker:
    def __init__(self, min_time_to_complete, max_time_to_complete):
        self.jobs_running = []
        self.finished_jobs = []
        self.time_to_complete = random.randint(min_time_to_complete, max_time_to_complete)
        self.time_left = 0

    #Converts worker to string
    def __str__(self):
        return "Jobs Running: " + str(self.jobs_running) + ", Time Left: " + str(self.time_left)

#Generator classes needed to call generate function
class Generator:
    def __init__(self, num_jobs, min_job_size, max_job_size, num_workers, min_time_to_complete_slow, max_time_to_complete_slow, min_time_to_complete_fast, max_time_to_complete_fast, bernoulli_prob, poisson_mean):
        self.num_jobs = num_jobs
        self.min_job_size = min_job_size
        self.max_job_size = max_job_size
        self.num_workers = num_workers
        self.min_time_to_complete_slow = min_time_to_complete_slow
        self.max_time_to_complete_slow = max_time_to_complete_slow
        self.min_time_to_complete_fast = min_time_to_complete_fast
        self.max_time_to_complete_fast = max_time_to_complete_fast
        self.bernoulli_prob = bernoulli_prob
        self.poisson_mean = poisson_mean
        self.id = 0

    def generate(self):
        #Generate numJobs new jobs of random (uniform) size
        jobs = []
        for _ in range(self.num_jobs):
            jobs.append(Job(self.id, random.randint(self.min_job_size, self.max_job_size)))
            self.id += 1

        #Generate numWorkers new workers: Half are "fast", half are "slow"
        workers = []
        for _ in range(int(self.num_workers / 2)):
            workers.append(Worker(self.min_time_to_complete_slow, self.max_time_to_complete_slow))
            workers.append(Worker(self.min_time_to_complete_fast, self.max_time_to_complete_fast))
        #If number of workers is odd, add an extra fast worker
        if int(self.num_workers / 2) % 2 == 1:
            workers.append(Worker(self.min_time_to_complete_fast, self.max_time_to_complete_fast))
        
        #Sort workers from fastest to slowest
        workers.sort(key=operator.attrgetter('time_to_complete'))

        return [jobs, workers]

    #Generates an additional job to be added with given probability.
    def gen_additional_jobs_bernoulli(self):
        #Generate a random number between 0 and 1 and compare it to prob to see if a job is added
        rand = random.uniform(0, 1)
        new_job = []
        if rand <= self.bernoulli_prob:
            new_job.append(Job(self.id, random.randint(self.min_job_size, self.max_job_size)))
            self.id += 1
        return new_job

    #Generates a random number of jobs based on a poisson distrubution to be added
    def gen_additional_jobs_poisson(self):
        #Generate a random number using poisson distribution to decide number of jobs being added (round down)
        new_jobs = []
        for _ in range(np.random.poisson(self.poisson_mean, 1)[0]):
            new_jobs.append(Job(self.id, random.randint(self.min_job_size, self.max_job_size)))
            self.id += 1
        return new_jobs