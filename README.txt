This simulation is used to compare the performance (i.e. Elapsed Processing Time and Average Time Per Job)
of various policies - SRPT, FIFO, Round Robin, Max Weight, Gittins (for single worker) - for a job batching scheduler. 

It was completed as part of an undergraduate research project

To run the simulator, several variables must be defined by the user:

Number of Jobs: The initial number of jobs that each scheduler is tasked with completing (all randomly generated)
Minimum Job Size: The smallest size that a randomly generated job can have
Maximum Job Size: The largest size that a randomly generated job can have
Number of Workers: The number of workers (e.g. GPUs on a server) that can complete jobs (all randomly generated,
    half are slower and half are faster)
Minimum Time to Complete for Slow Workers: The smallest amount of time that a randomly generated slow worker needs to 
    complete a task
Maximum Time to Complete for Slow Workers: The largest amount of time that a randomly generated slow worker needs to 
    complete a task
Minimum Time to Complete for Fast Workers: The smallest amount of time that a randomly generated fast worker needs to 
    complete a task
Maximum Time to Complete for Fast Workers: The largest amount of time that a randomly generated fast worker needs to 
    complete a task
Batch Size: The Number of jobs that can be batched together at a time and sent to a worker for completion
Memory Copy Cost Factor: The multiplicative constant that adds time to a worker's completion time if an already worked
    on job is sent to a new worker (normally 0)
Memory Copy Cost Constant: The additive constant that adds time to a worker's completion time if an already worked
    on job is sent to a new worker
Bernoulli Distribution Probability: The probability for a Bernoulli distribution which will randomly create additional
    jobs for the schedulers to complete (if 0, no new jobs will be added)
Poisson Distribution Mean Value: The mean value for a Poisson distribution which will randomly create additional jobs  
    for the schedulers to complete (if 0, no new jobs will be added)

To test the effect that each of these values has on each scheuling policy, the user can change set the 'varied_parameter'
raneg of values and in line 51 set the parameter they wish to change equal to int(varied_parameter[i]) (i.e. simply change 
the name of the first variable in this line of code). From here, the simulation will run as many times as there are values
in the varied_parameter list, changing the desired parameter on each run. The Total Elapsed Time, Average Time Taken per
Job, and Total Memory Copy Cost for each scheduler is recorded after each run. These values are then stored in a CSV called
Output.csv (Elapsed Time for each scheduler is recorded first, then Average Time, then Memory Copy Cost). This CSV file
can then be used by the Matlab DataPlotting.m script to generate graphs for each of the metrics recorded (Plotting using 
Python is currently commented out but can be reused with some alterations to the code). 
