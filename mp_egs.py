# -*- coding: utf-8 -*-
"""
Created on Tue Feb 25 21:01:45 2020

@author: jrbrad
"""

import multiprocessing as mp
import time
import random
import sys

def calculate(func, args):
    result = func(*args)
    return '%s says that %s%s = %s' % (
        mp.current_process().name,
        func.__name__, args, result
        )

def calculatestar(args):
    return calculate(*args)

def mul(a, b):
    time.sleep(0.5 * random.random())
    return a * b

def plus(a, b):
    time.sleep(0.5 * random.random())
    return a + b

def pow3(x):
    time.sleep(2.0 * random.random())
    return x ** 3


def test():
    PROCESSES = 2 #4
    print('Creating pool with %d processes\n' % PROCESSES)

    with mp.Pool(PROCESSES) as pool:
        ''' Arguments data
              - _lot = List of tuples
              - _w_func function name is included with arguments '''
        args_lot_w_func = [(mul, (i, 7)) for i in range(10)] + \
                [(plus, (i, 8)) for i in range(10)]

        args_lot = [(i, 7) for i in range(10)]
        
        args_single = [i for i in range(10)]		
        
        ''' Statements assigning multiple processes '''
        ''' .map() 
              - supports functions with only one argument 
              - blocks execution until all processes are complete 
              - results returned in order of the passed arguments 
              - returns a list'''
        results_map = pool.map(pow3, args_single)
        
        print('map() results:')
        print(results_map,'\n') 
        
        ''' How to circumvent the single argument restriction '''
        print('Ordered results using pool.map() --- will block till complete:')
        for x in pool.map(calculatestar, args_lot_w_func):
            print('\t', x)
        print()
        
        
        ''' .map_async() 
              - asynchronous version of .map 
              - returns a multiprocessing.pool.MapResult (need ,get() to access '''
        results_map_async = pool.map_async(pow3, args_single)
        
        print('map_async() results:')
        print(results_map_async.get(),'\n')
        print()
        
        
        ''' .starmap() supports functions with multiple arguments 
              - returns a list '''
        results_starmap = pool.starmap(mul, args_lot)
        
        print('starmap() results:')
        for r in results_starmap:
            print('\t', r)
        print()
        
        
        ''' Results not necessarily return in sequnce of arguments 
              - returns multiprocessing.pool.ApplyResult  '''
        results_async = [pool.apply_async(calculate, t) for t in args_lot_w_func]
        
        print('Ordered results using pool.apply_async():')
        for r in results_async:
            print('\t', r.get())
        print()

        
        ''' A lazier version of .map() 
              - returns list '''
        results_imap = pool.imap(calculatestar, args_lot_w_func)
        
        print('Ordered results using pool.imap():')
        for x in results_imap:
            print('\t', x)
        print()

        
        ''' Same as imap() but results not necessarily sequential 
              - returns list '''
        results_imap_unordered = pool.imap_unordered(calculatestar, args_lot_w_func)
                
        print('Unordered results using pool.imap_unordered():')
        for x in results_imap_unordered:
            print('\t', x)
        print()

       
        ''' Print results of various alternatives '''
        
        
if __name__ == '__main__':
    test()