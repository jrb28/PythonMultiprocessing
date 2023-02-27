# -*- coding: utf-8 -*-
"""
Created on Thu Apr 06 09:39:58 2017

@author: jrbrad
"""

import multiprocessing as mp
import time

def factorial(num):
    if num ==1:
        return 1
    else:
        return num * factorial(num-1)
        
def call_fact(num):
    return str(num) + '! = ' + str(factorial(num))
    
if __name__ == '__main__':
    values = range(50,100)
    num_cores = 5
    pool = mp.Pool(num_cores)         # create process pool using 6 CPUs
    time_start = time.time()
    #results = pool.map(call_fact,values)  # creates a separate process for each value in values list
    results = pool.map(call_fact,values)  # creates a separate process for each value in values list
    pool.close()         # done creating processes in pool
    pool.join()          # wait until all processes are complete before continuing
    exec_time = time.time() - time_start
    print('\nOrdered results using pool.imap() and ' + str(num_cores) + ' cores:')        
    #for x in results:
    #    print('\t', x)
    print("Execution time:",exec_time, '\n')
    
    time_start = time.time()
    for val in values:
        #print(call_fact(val))
        call_fact(val)
    exec_time = time.time() - time_start
    print("Execution time on 1 CPU:",exec_time)