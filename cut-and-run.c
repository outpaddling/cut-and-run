/***************************************************************************
 *  Description:
 *      Process a file in pieces using multiple threads
 *
 *  History: 
 *  Date        Name        Modification
 *  2021-04-25  Jason Bacon Begin
 ***************************************************************************/

#include <stdio.h>
#include <sysexits.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <omp.h>
#include "cut-and-run.h"

int     main(int argc,char *argv[])

{
    char        *filename;
    static long *start_positions;
    FILE        *infile;
    char        *thread_count_str,
		*end;
    unsigned    thread_count;
    
    switch(argc)
    {
	case    2:
	    filename = argv[1];
	    break;
	default:
	    fprintf(stderr,"Usage: [env OMP_NUM_THREADS=#] %s\n",argv[0]);
	    break;
    }
    
    // Get thread count from environment if present, else default
    if ( (thread_count_str = getenv("OMP_NUM_THREADS")) == NULL )
    {
	thread_count = DEFAULT_THREAD_COUNT;
	omp_set_num_threads(thread_count);
    }
    else
    {
	thread_count = strtoul(thread_count_str, &end, 10);
	if ( *end != '\0' )
	{
	    fprintf(stderr, "Invalid OMP_NUM_THREADS: %s.\n", thread_count_str);
	    return EX_DATAERR;
	}
    }
    printf("%u threads\n", thread_count);

    if ( (infile = fopen(filename, "r")) == NULL )
    {
	fprintf(stderr, "%s: Cannot open %s: %s\n", argv[0], filename,
		strerror(errno));
	return EX_NOINPUT;
    }
    start_positions = find_start_positions(infile, thread_count);
    return spawn_processes(filename, start_positions, thread_count);
}


/***************************************************************************
 *  Description:
 *      Find the starting position within the input file for each thread.
 *      The file is divided into thread_count blocks of N lines and the
 *      starting position for each thread is the beginning of the first
 *      line in the block.
 *
 *  History: 
 *  Date        Name        Modification
 *  2021-04-25  Jason Bacon Begin
 ***************************************************************************/

long    *find_start_positions(FILE *infile, unsigned thread_count)

{
    long    *start_positions,
	    eof_position;
    int     ch;
    size_t  c,
	    c2,
	    total_lines,
	    lines_per_thread,
	    max_lines = 1000000;
    
    // Allocate conservatively and add on as needed
    if ( (start_positions = malloc(max_lines * sizeof(*start_positions))) == NULL )
    {
	fputs("find_start_positions(): Cannot allocate start_positions.\n", stderr);
	exit(EX_UNAVAILABLE);
    }
    
    start_positions[0] = 0;     // First block is beginning of file
    for (total_lines = 1; (ch = getc(infile)) != EOF; )
    {
	if ( ch == '\n' )
	    start_positions[total_lines++] = ftell(infile);
	if ( total_lines == max_lines )
	{
	    max_lines *= 2;
	    start_positions = realloc(start_positions, max_lines * sizeof(*start_positions));
	    if ( start_positions == NULL )
	    {
		fputs("find_start_positions(): Cannot allocate start_positions.\n", stderr);
		exit(EX_UNAVAILABLE);
	    }
	}
    }
    eof_position = ftell(infile) + 1;
    
    lines_per_thread = total_lines / thread_count + 1;
    printf("Lines per thread: %zu\n", lines_per_thread);
    
    /*
     *  Rewinding is not enough.  Private thread FILE structures must be
     *  created so that each stream has a different file descriptor.
     */
    fclose(infile);
    
    // Move the start positions for each process to the top of the list
    for (c = 0, c2 = 0; c < total_lines; c += lines_per_thread)
    {
	start_positions[c2] = start_positions[c];
	// printf("%zu %lu\n", c2, start_positions[c2]);
	++c2;
    }
    start_positions[c2] = eof_position;
    
    /*
     *  Immediately free memory from unused line starts to make Ray happy
     *  This cannot fail since we're shrinking the array
     */
    start_positions = realloc(start_positions,
			      (thread_count + 1) * sizeof(*start_positions));
    return start_positions;
}


int     spawn_processes(char *filename, long start_positions[], unsigned thread_count)

{
    #pragma omp parallel for
    for (unsigned thread = 0; thread < thread_count; ++thread)
    {
	unsigned    thread_id;
	long        my_start,
		    my_end;
	char        pipe_cmd[CMD_MAX + 1] = "";
	FILE        *outfile,
		    *infile;
	int         c;
	
	// Verify that OpenMP has the right thread count
	thread_id = omp_get_thread_num();
	
	// Copy FILE structure to private variables so they can diverge
	infile = fopen(filename, "r");
	
	// Open a pipe with popen() or a named pipe with fopen()
	snprintf(pipe_cmd, CMD_MAX, "cat > thread%u", thread);
	if ( (outfile = popen(pipe_cmd, "w")) == NULL )
	{
	    fprintf(stderr, "spawn_processes(): Cannot pipe output: %s\n",
		    pipe_cmd);
	    exit(EX_CANTCREAT);
	}
	
	// Send chars from this thread's section of the file to the pipe
	my_start = start_positions[thread];
	my_end = start_positions[thread + 1];
	fseek(infile, my_start, SEEK_SET);
	
	printf("Thread #%u (%u) sending characters %lu to %lu to %s\n",
		thread, thread_id, my_start, my_end, pipe_cmd);
	for (c = my_start; c < my_end; ++c)
	    putc(getc(infile), outfile);
	
	pclose(outfile);
	fclose(infile);
    }
    return EX_OK;
}
