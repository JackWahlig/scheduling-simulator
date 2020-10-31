from Generator import Generator
from Scheduler import *
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import copy 
import csv

#This main function drives the simulation for data collection.
def main():  
    #Values which define the simulaiton
    num_jobs = 250
    min_job_size = 5
    max_job_size = 10
    num_workers = 10
    min_time_to_complete_slow = 5
    max_time_to_complete_slow = 5
    min_time_to_complete_fast = 5
    max_time_to_complete_fast = 5
    batch_size = 5
    mem_copy_cost_factor = 0 
    mem_copy_cost_constant = 5
    bernoulli_prob = 0
    poisson_mean = 0

    #This variable will act as the parameter which we will change. Instead of taking the above parameter, we will insert
    #these values into the correct position in the simulation function. 
    #FOR NOW, THE PARAMETER YOU WISH TO CHANGE MUST BE ALTERED MANUALLY
    lower_value = 5
    upper_value = 50
    varied_parameter = np.linspace(lower_value, upper_value, upper_value - lower_value + 1) 
    
    #We run a loop, collecting data from each trial as we vary the parameter above, storing necessary data after each simulation
    SRPT_elapsed = []
    FIFO_elapsed = []
    RR_elapsed = []
    MW_elapsed = []
    Gittins_elapsed = []
    SRPT_avg_time = []
    FIFO_avg_time = []
    RR_avg_time = []
    MW_avg_time = []
    Gittins_avg_tome = []
    SRPT_copy_cost = []
    FIFO_copy_cost = []
    RR_copy_cost = []
    MW_copy_cost = []
    Gittins_copy_cost = []
    
    for i in range(len(varied_parameter)):
        num_workers = int(varied_parameter[i]) #Here is where you set the parameter you wish to vary
        results = run_simulation(num_jobs, min_job_size, max_job_size, num_workers, min_time_to_complete_slow, max_time_to_complete_slow, min_time_to_complete_fast, max_time_to_complete_fast, batch_size, mem_copy_cost_factor, mem_copy_cost_constant, bernoulli_prob, poisson_mean)
        print(results)
        #Total Elapsed Time
        SRPT_elapsed.append(results[0][0])
        FIFO_elapsed.append(results[1][0])
        RR_elapsed.append(results[2][0])
        MW_elapsed.append(results[3][0])
        Gittins_elapsed.append(results[4][0])
        #Average Time per Job
        SRPT_avg_time.append(results[0][1])
        FIFO_avg_time.append(results[1][1])
        RR_avg_time.append(results[2][1])
        MW_avg_time.append(results[3][1])
        Gittins_avg_tome.append(results[4][1])
        #Total Memory Copy Cost
        SRPT_copy_cost.append(results[0][2])
        FIFO_copy_cost.append(results[1][2])
        RR_copy_cost.append(results[2][2])
        MW_copy_cost.append(results[3][2])
        Gittins_copy_cost.append(results[4][2])

    #Write to CSV file
    with open('Output.csv', mode='w') as output_file:
        output_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        output_writer.writerow(varied_parameter) #X-values to plot
        output_writer.writerow(SRPT_elapsed)
        output_writer.writerow(FIFO_elapsed)
        output_writer.writerow(RR_elapsed)
        output_writer.writerow(MW_elapsed)
        #output_writer.writerow(SRPT_elapsed)
        output_writer.writerow(SRPT_avg_time)
        output_writer.writerow(FIFO_avg_time)
        output_writer.writerow(RR_avg_time)
        output_writer.writerow(MW_avg_time)
        #output_writer.writerow(SRPT_elapsed)
        output_writer.writerow(SRPT_copy_cost)
        output_writer.writerow(FIFO_copy_cost)
        output_writer.writerow(RR_copy_cost)
        output_writer.writerow(MW_copy_cost)
        #output_writer.writerow(SRPT_elapsed)


    #Plotting the data - All plot titles and labels must be manually input
    #fig = plt.figure()
    #plt.plot(varied_parameter, SRPT_data)
    #plt.plot(varied_parameter, FIFO_data)
    #plt.plot(varied_parameter, RR_data)
    #plt.plot(varied_parameter, MW_data)
    #plt.plot(varied_parameter, Gittins_data) NEED TO ADD CHECKS FOR GITTINS TODO

    #plt.legend(["SRPT", "FIFO", "Round Robin", "Max Weight"])

    #fig.suptitle('Average Time Per Job as a Function of Number of Jobs', fontsize=20)
    #plt.xlabel('Number of Jobs', fontsize=18)
    #plt.ylabel('Total Elapsed Time', fontsize=18)
    
    #lower_bound = min(min(SRPT_data), min(FIFO_data), min(RR_data), min(MW_data)) #These are just for plotting purposes
    #upper_bound = max(max(SRPT_data), max(FIFO_data), max(RR_data), max(MW_data))

    #plt.xlim(min(varied_parameter), max(varied_parameter))
    #plt.ylim(lower_bound - 1, upper_bound + 1)
    #plt.xticks(np.arange(min(varied_parameter), max(varied_parameter), step=(len(varied_parameter) / 10)))
    #plt.yticks(np.arange(lower_bound - 1, upper_bound + 1, step=(upper_bound - lower_bound) / 10))
    #plt.show()    


#This function takes in all simulation parameters and runs a simulation of all possible (Gittins may not be) Schedulers.
#The functions returns a list containing lists with metrics (Total Elapsed Time, Average time per Job, and 
#total Memory Copy Cost) on each Scheduler in order: SRTP, FIFO, Round Robin, Max Weight, and Gittins. 
def run_simulation(num_jobs, min_job_size, max_job_size, num_workers, min_time_to_complete_slow, max_time_to_complete_slow, min_time_to_complete_fast, max_time_to_complete_fast, batch_size, mem_copy_cost_factor, mem_copy_cost_constant, bernoulli_prob, poisson_mean):    
    #Use Generator to generate jobs and workers
    G = Generator(num_jobs, min_job_size, max_job_size, num_workers, min_time_to_complete_slow, max_time_to_complete_slow, min_time_to_complete_fast, max_time_to_complete_fast, bernoulli_prob, poisson_mean)
    temp = G.generate()
    jobs = temp[0]
    workers = temp[1]

    #Make Schedulers with generated jobs and workers and add them to a list
    SRPT = SRPTScheduler(copy.deepcopy(jobs), copy.deepcopy(workers), batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
    FIFO = FIFOScheduler(copy.deepcopy(jobs), copy.deepcopy(workers), batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
    RR = RoundRobinScheduler(copy.deepcopy(jobs), copy.deepcopy(workers), batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
    MW = MaxWeightScheduler(copy.deepcopy(jobs), copy.deepcopy(workers), batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
    Gittins = GittinsScheduler(copy.deepcopy(jobs), copy.deepcopy(workers), batch_size, mem_copy_cost_factor, mem_copy_cost_constant)
    
    #Only use Gittins for batch sizes of 1
    if batch_size == 1:
        Schedulers = [SRPT, FIFO, RR, MW, Gittins]
    else:
        Schedulers = [SRPT, FIFO, RR, MW]

    #Run all schedulers
    #Run global loop until all jobs are done
    finished = False
    while not finished > 0:
        #Potentially generate a new job using Bernoulli and Poisson Distributions
        new_jobs = G.gen_additional_jobs_bernoulli()
        new_jobs += G.gen_additional_jobs_poisson()

        #Go through each scheduler, one at a time
        for s in range(len(Schedulers)):
            #Only continue if the scheduler is not finished or if a new jobs has come in 
            if len(Schedulers[s].jobs) > 0 or len(new_jobs) > 0:
                Schdlr = Schedulers[s] #The current scheduler we are using this iteration
                empty_workers = [] #Used to keep track of which workers need new jobs
                #If a new job has come in, append it to the end of the jobs list
                if not new_jobs == -1:
                    Schdlr.jobs += copy.deepcopy(new_jobs)

                #Go through each worker and check if it is empty/time_left = 0
                for i in range (len(Schdlr.workers)):
                    worker = Schdlr.workers[i]
                    #If worker is done, decrement size of that job (seacrh by id) and batch new jobs. Reset time_left to time_to_complete
                    if worker.time_left == 0:
                        index = 0
                        while index < len(Schdlr.jobs):
                            job = Schdlr.jobs[index]
                            if job.id in worker.jobs_running:
                                job.size -= 1
                                job.amount_processed += 1
                                #If job is finished, remove it from the list (decrement index to keep up with this)
                                if job.size == 0:
                                    del Schdlr.jobs[index]
                                    Schdlr.jobs_finished += 1
                                    index -= 1
                            index += 1
                        empty_workers.append(worker) #Add this worker to list of empty workers
                    #Decrease time_left by 1
                    else:
                        worker.time_left -= 1
                #Batch new jobs for the empty workers
                Schdlr.batch_jobs(empty_workers)
                Schdlr.elapsed_time += len(Schdlr.jobs)
                Schdlr.time_taken += 1 

        #Check if all schedulers are done
        finished = True
        for i in range(len(Schedulers)):
            if len(Schedulers[i].jobs) > 0:
                finished = False
                break

        #Testing

        #print("MW Time: " + str(MW.time_taken))
        #MW.print_jobs()
        #MW.print_workers()

        #print("RR Time: " + str(RR.time_taken))
        #RR.print_jobs()
        #RR.print_workers() 

        #print("FIFO Time: " + str(FIFO.time_taken))
        #FIFO.print_jobs()
        #FIFO.print_workers()  

        #print("SRPT Time: " + str(SRPT.time_taken))
        #SRPT.print_jobs()
        #SRPT.print_workers()  

        # print("Gittins Time: " + str(Gittins.time_taken))
        #Gittins.print_jobs()
        #Gittins.print_workers()  

    SRPT_metrics = [SRPT.elapsed_time, SRPT.time_taken/num_jobs, SRPT.total_mem_copy_cost]
    FIFO_metrics = [FIFO.elapsed_time, FIFO.time_taken/num_jobs, FIFO.total_mem_copy_cost]
    RR_metrics = [RR.elapsed_time, RR.time_taken/num_jobs, RR.total_mem_copy_cost]
    MW_metrics = [MW.elapsed_time, MW.time_taken/num_jobs, MW.total_mem_copy_cost]
    Gittins_metrics = [Gittins.elapsed_time, Gittins.time_taken/num_jobs, Gittins.total_mem_copy_cost]

    return [SRPT_metrics, FIFO_metrics, RR_metrics, MW_metrics, Gittins_metrics]

if __name__ == "__main__":
    main()