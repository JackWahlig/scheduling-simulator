from abc import ABC, abstractmethod
import random
import operator

#Abstract class that defines a scheduler. Takes in jobs, workers, and batchsize
class Scheduler(ABC):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        self.jobs = jobs
        self.workers = workers
        self.batch_size = batch_size
        self.mem_copy_cost_factor = mem_copy_cost_factor
        self.mem_copy_cost_constant = mem_copy_cost_constant
        self.jobs_finished = 0
        self.elapsed_time = 0
        self.total_mem_copy_cost = 0
        self.time_taken = 0

    #Prints out all current jobs and their sizes
    def print_jobs(self):
        for i in range(len(self.jobs)):
            print(self.jobs[i])

    #Prints out all workers, their running jobs, and time left
    def print_workers(self):
        for i in range(len(self.workers)):
            print(self.workers[i])
    
    #Checks to see if a given job by ID is currently running in any worker
    def is_running(self, id):
        for i in range(len(self.workers)):
            if id in self.workers[i].jobs_running:
                return True
        return False

    #Gets the next jobs to be batched
    def get_new_batch(self):
        num_batched_jobs = 0
        batch = []
        index = 0
        #Stop when batch size is met or until no more jobs can be batched
        while num_batched_jobs < self.batch_size and index < len(self.jobs):
            job = self.jobs[index]
            if not self.is_running(job.id):
                batch.append(job)
                num_batched_jobs += 1
            index += 1
        
        return batch

    #Checks if new jobs are in old jobs in worker. If not, adds memory copy cost to time_left
    def mem_copy_cost(self, worker, jobs):
        mem_copy_cost = 0
        for i in range(len(jobs)):
            if not (jobs[i].id in worker.finished_jobs) and (jobs[i].amount_processed > 0):
                mem_copy_cost += (self.mem_copy_cost_factor * jobs[i].amount_processed + self.mem_copy_cost_constant)
        return mem_copy_cost

    #Randomly assign a worker for this batch
    def assign_random_worker(self, empty_workers, batch):
        index = random.randint(0, len(empty_workers) - 1)
        for i in range(len(batch)):
            empty_workers[index].jobs_running.append(batch[i].id)
        empty_workers[index].time_left = self.mem_copy_cost(empty_workers[index], batch) + empty_workers[i].time_to_complete
        self.total_mem_copy_cost += self.mem_copy_cost(empty_workers[index], batch)
        empty_workers.remove(empty_workers[i])

    #Place this batch in the worker with the best (time to complete + mem copy cost)
    def assign_best_worker(self, empty_workers, batch):
        best_worker = empty_workers[0]
        best_time_to_complete = self.mem_copy_cost(best_worker, batch) + best_worker.time_to_complete
        for i in range(1, len(empty_workers)):
            #Get mem copy cost of worker and compare to best time to complete so far
            time_to_complete_temp = self.mem_copy_cost(empty_workers[i], batch) + empty_workers[i].time_to_complete
            if time_to_complete_temp < best_time_to_complete:
                best_worker = empty_workers[i]
                best_time_to_complete = time_to_complete_temp
        
        #Batch the job to the best worker, add time left, and remove it from empty_workers
        for i in range(len(batch)):
            best_worker.jobs_running.append(batch[i].id)
        best_worker.time_left = best_time_to_complete
        self.total_mem_copy_cost += self.mem_copy_cost(best_worker, batch)
        empty_workers.remove(best_worker)

#Scheduler that uses SRPT to batch jobs
class SRPTScheduler(Scheduler):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        super().__init__(jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
        #SRPT sorts all of the jobs it gets immediately by size
        self.jobs.sort(key=operator.attrgetter('size'))

    #Uses SRPT to obtain the next jobs to batch together and sends them to the worker
    #Cannot batch jobs that are already in a worker
    def batch_jobs(self, empty_workers):
        #Resort the jobs list in case the order is incorrect
        self.jobs.sort(key=operator.attrgetter('size'))

        #Go through and empty all jobs/update finished jobs
        for i in range(len(empty_workers)):
            empty_workers[i].finished_jobs = empty_workers[i].jobs_running
            empty_workers[i].jobs_running = []

        #Repeat until all workers have new jobs
        while len(empty_workers) != 0:
            #Get new batch of jobs
            batch = self.get_new_batch()

            #Find worker for the batch
            if len(batch) > 0:   
                #Pick random worker to batch ignoring mem copy cost
                #self.assign_random_worker(empty_workers, batch)

                #Iterate through each worker and find one with smallest memory cost + time to complete for this batch
                self.assign_best_worker(empty_workers, batch)

            #If batch is empty, do not batch the worker (avoids it running while doing nothing)
            else:
                break

#Scheduler that uses FIFO to bacth jobs
class FIFOScheduler(Scheduler):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        super().__init__(jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant)

    #Uses FIFO to obtain the next jobs to batch together and sends them to the worker
    #Cannot batch jobs that are already in a worker
    def batch_jobs(self, empty_workers):
        #Go through and empty all jobs/update finished jobs
        for i in range(len(empty_workers)):
            empty_workers[i].finished_jobs = empty_workers[i].jobs_running
            empty_workers[i].jobs_running = []

        #Repeat until all workers have new jobs
        while len(empty_workers) != 0:
            #Get new batch of jobs
            batch = self.get_new_batch()

            #Find worker for batch
            if len(batch) > 0:
                #Pick random worker to batch ignoring mem copy cost
                #self.assign_random_worker(empty_workers, batch)

                #Iterate through each worker and find one with smallest memory cost for this batch
                self.assign_best_worker(empty_workers, batch)

            #If batch is empty, do not batch the worker (avoids it running while doing nothing)
            else:
                break

#Scheduler that uses round robin policy to batch jobs
class RoundRobinScheduler(Scheduler):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        super().__init__(jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant)

    #After a job is added to the batch, the jobs need to rotate round-robin style
    def get_new_batch(self):
        num_batched_jobs = 0
        batch = []
        observed = 0
        #Stop when batch size is met or until we've looked at every job
        while num_batched_jobs < self.batch_size and observed < len(self.jobs):
            job = self.jobs[0]
            if not self.is_running(job.id):
                batch.append(job)
                num_batched_jobs += 1
            self.jobs = self.jobs[1:] + self.jobs[:1] #Rotate the list of jobs left by 1
            observed += 1
        
        return batch
    
    #Uses Round Robin to obtain the next jobs to batch together and sends them to the worker
    #Cannot batch jobs that are already in a worker
    def batch_jobs(self, empty_workers):
        #Go through and empty all jobs/update finished jobs
        for i in range(len(empty_workers)):
            empty_workers[i].finished_jobs = empty_workers[i].jobs_running
            empty_workers[i].jobs_running = []

        #Repeat until all workers have new jobs
        while len(empty_workers) != 0:
            #Get new batch of jobs
            batch = self.get_new_batch()

            #Find worker for batch
            if len(batch) > 0:
                #Pick random worker to batch ignoring mem copy cost
                #self.assign_random_worker(empty_workers, batch)

                #Iterate through each worker and find one with smallest memory cost for this batch
                self.assign_best_worker(empty_workers, batch)

            #If batch is empty, do not batch the worker (avoids it running while doing nothing)
            else:
                break

#Scheduler that uses Max Weight policy to batch jobs
class MaxWeightScheduler(Scheduler):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        super().__init__(jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
        #Max Weight sorts all of the jobs it gets immediately by size in reverse order
        self.jobs.sort(key=operator.attrgetter('size'))
        self.jobs.reverse()

    #Uses SRPT to obtain the next jobs to batch together and sends them to the worker
    #Cannot batch jobs that are already in a worker
    def batch_jobs(self, empty_workers):
        #Resort the jobs list in case the order is incorrect
        self.jobs.sort(key=operator.attrgetter('size'))
        self.jobs.reverse()

        #Go through and empty all jobs/update finished jobs
        for i in range(len(empty_workers)):
            empty_workers[i].finished_jobs = empty_workers[i].jobs_running
            empty_workers[i].jobs_running = []

        #Repeat until all workers have new jobs
        while len(empty_workers) != 0:
            #Get new batch of jobs
            batch = self.get_new_batch()

            #Find worker for the batch
            if len(batch) > 0:   
                #Pick random worker to batch ignoring mem copy cost
                #self.assign_random_worker(empty_workers, batch)

                #Iterate through each worker and find one with smallest memory cost + time to complete for this batch
                self.assign_best_worker(empty_workers, batch)

            #If batch is empty, do not batch the worker (avoids it running while doing nothing)
            else:
                break

#Scheduler that uses Gittins Index policy to batch jobs
class GittinsScheduler(Scheduler):
    def __init__(self, jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant):
        super().__init__(jobs, workers, batch_size, mem_copy_cost_factor, mem_copy_cost_constant)

    #Must look through all jobs to find one(s) with highest Gittins index
    def get_new_batch(self):
        #Create parallel array that holds all Gittins indices
        gittins = []
        beta = 0.5
        r = 1
        for i in range(len(self.jobs)):
            job = self.jobs[i]
            gittins.append((r * (beta**job.size) * (1 - beta)) / (1 - beta**job.size))
        
        #Batch jobs with largest Gittins index, keeping track of which ones are already running
        num_batched_jobs = 0
        batch = []
        running_jobs = []
        #Stop when batch size is met or until we've looked at every job
        while num_batched_jobs < self.batch_size and len(running_jobs) < len(self.jobs):
            largest_gittins_index = -1
            for i in range(len(gittins)):
                #Find first job not already running
                if largest_gittins_index == -1 and i not in running_jobs:
                    largest_gittins_index = i

                #Compare Gittins list to find largest
                elif gittins[i] > gittins[largest_gittins_index] and i not in running_jobs:
                    largest_gittins_index = i
                    
            job = self.jobs[largest_gittins_index]
            if not self.is_running(job.id):
                batch.append(job)
                num_batched_jobs += 1
            else:
                running_jobs.append(largest_gittins_index)
        
        return batch

    #Uses Gittins Index to obtain the next jobs to batch together and sends them to the worker (batch size = 1)
    #Cannot batch jobs that are already in a worker
    def batch_jobs(self, empty_workers):
        #Go through and empty all jobs/update finished jobs
        for i in range(len(empty_workers)):
            empty_workers[i].finished_jobs = empty_workers[i].jobs_running
            empty_workers[i].jobs_running = []

        #Repeat until all workers have new jobs
        while len(empty_workers) != 0:
            #Get new batch of jobs
            batch = self.get_new_batch()

            #Find worker for the batch
            if len(batch) > 0:   
                #Pick random worker to batch ignoring mem copy cost
                #self.assign_random_worker(empty_workers, batch)

                #Iterate through each worker and find one with smallest memory cost + time to complete for this batch
                self.assign_best_worker(empty_workers, batch)

            #If batch is empty, do not batch the worker (avoids it running while doing nothing)
            else:
                break