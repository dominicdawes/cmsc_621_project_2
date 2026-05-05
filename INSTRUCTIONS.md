#\ Project 2

Extend the GoLang project 1 by distributing the workers as independent processes (running on
possible different machines) communicating with the dispatcher, consolidator, and a fileserver
via gRPC over TCP.

Your implementation should be correct and efficient:
1. Create a GoLang fileserver process that allows workers to fetch a segment of a file in
chunks via a gRPC call. The fileserver implements server--side streaming gRPC for
providing the chunks of a segment. The Fileserver process is started and terminated
explicitly by the user.
2. Each worker makes a unary gRPC call to the dispatcher to pull a job from its jobqueue .
Then, the worker makes a gPRC call to fetch the contents of the job's segment in chunks,
and upon receiving a chunk, it finds the number of primes in it. Upon fetching all the
segment's chunks, the worker makes an gRPC call to the consolidator pushing (telling it)
the total number of primes in the segment. Workers are started explicitly by the user, and
terminate when the consolidator "closes" the resultsqueue .
3. The dispatcher and consolidator are GoLang goroutines of your main process acting as two
distinct servers for the workers. The dispatcher and consolidator listen to different ports.
4. The address of the dispatcher, consolidator, and fileserver are store in a simple ASCII
configuration file accessible to all processes, eg. filepath ./primes_config.txt The format of
the configuration is the name of the server following by its IP address/name and port
number on a single line, eg:
dispatcher host0.bogus.net 5001
consolidator host0.bogus.net 5002
fileserver host1.bogus.net 5003
5. Your main .go program should accept N, C, data and config filepaths as command-line
arguments.
6. Your worker .go main program should accept C and the config filepath as command-line
arguments.
7. For simplicity, you may run all your processes on the same machine (localhost), provided
they only communicate via gRPC calls.
your main program computes the correct number of primes in the datafile.

Your implementation should be correct and efficient
- your main program computes the correct number of primes in the datafile
- there are no race conditions.
- achieves maximum concurrency, i.e. least synchronization delays and/or latencies.
- has least amount of information (#bytes) communicated among the threads/processe

## Project Report

Write a short report describing your design and implementation choices, and answer the
following questions:

1. What is the least (expected) elapsed (wall) time for a random datafile of 1GB in your
implementation with M workers?
2. What is the largest (random) datafile you can process within 3 mins (wall) elapsed time with
M workers?
3. How does the elapsed time change as a function of M, for the datafile in Q2 and the N, C
parameter values?

For each question Q1-Q3 above, provide the setting (command-line arguments) for all M, N,
and C parameters, and the basic statistics (min, max, average, median) of the number of jobs
completed by the workers, and the (wall) elapsed time (in msecs) for your main function.
Assume that N and C take values in some small predefined sets of values:

eg. N in {1KB, 32KB, 64KB, 256KB, 1MB, 64MB}; C in {64B, 1KB, 4KB, 8KB}; and M in {4, 8,
16}.


## What to submit?
Submit a compressed zip/tar.gz archive that has a single folder with the following files (directly
under that folder - no subfolders etc)