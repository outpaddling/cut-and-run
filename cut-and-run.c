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
#include <fcntl.h>      // open()
#include <unistd.h>     // read()
#include <sys/param.h>  // MIN()
#include <sys/stat.h>   // fstat()
#include <omp.h>
#include "cut-and-run.h"

int     main(int argc,char *argv[])

{
    static long *start_positions;
    int         infd;
    char        *filename,
		*out_filename,
		*cmd,
		*thread_count_str,
		*end;
    unsigned    thread_count;
    
    switch(argc)
    {
	case    4:
	    filename = argv[1];
	    cmd = argv[2];
	    out_filename = argv[3];
	    break;
	default:
	    fprintf(stderr,
		    "Usage: [env OMP_NUM_THREADS=#] %s command output-filename\n",
		    argv[0]);
	    return EX_USAGE;
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

    if ( (infd = open(filename, O_RDONLY)) == -1 )
    {
	fprintf(stderr, "%s: Cannot open %s: %s\n", argv[0], filename,
		strerror(errno));
	return EX_NOINPUT;
    }
    // Doesn't help at all
    //setvbuf(infd, read_buff, _IOFBF, read_buff_size);
    start_positions = find_start_positions(infd, thread_count);
    return spawn_processes(filename, cmd, out_filename,
			   start_positions, thread_count);
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

long    *find_start_positions(int infd, unsigned thread_count)

{
    long    *start_positions,
	    // Tracking this is slightly faster than ftell()
	    file_position,
	    eof_position;
    int     ch;
    size_t  c,
	    c2,
	    total_lines,
	    lines_per_thread,
	    bytes,
	    max_lines = 1000000,
	    read_buff_size;
    char    *p,
	    *read_buff;
    struct stat fileinfo;
    
    // Allocate conservatively and add on as needed
    if ( (start_positions = malloc(max_lines * sizeof(*start_positions))) == NULL )
    {
	fputs("find_start_positions(): Cannot allocate start_positions.\n", stderr);
	exit(EX_UNAVAILABLE);
    }
    
    fstat(infd, &fileinfo);
    read_buff_size = fileinfo.st_blksize;
    printf("Block size = %zu\n", read_buff_size);
    
    if ( (read_buff = malloc(read_buff_size + 1)) == NULL )
    {
	fputs("find_start_positions(): Cannot allocate read_buff.\n", stderr);
	exit(EX_UNAVAILABLE);
    }

    file_position = 0;
    total_lines = 1;
    start_positions[0] = 0;     // First block is beginning of file
    while ( (bytes = read(infd, read_buff, read_buff_size)) > 0 )
    {
	for (p = read_buff; p < read_buff + bytes; ++p, ++file_position)
	{
	    if ( *p == '\n' )
	    {
		start_positions[total_lines++] = file_position;
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
	}
    }
    eof_position = file_position + 1;
    
    lines_per_thread = total_lines / thread_count + 1;
    printf("Lines per thread: %zu\n", lines_per_thread);
    
    /*
     *  Rewinding is not enough.  Private thread FILE structures must be
     *  created so that each stream has a different file descriptor.
     */
    close(infd);
    free(read_buff);
    
    // Move the start positions for each thread to the top of the list
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


int     spawn_processes(char *filename, char *cmd, char *out_filename,
			long start_positions[], unsigned thread_count)

{
    #pragma omp parallel for
    for (unsigned thread = 0; thread < thread_count; ++thread)
    {
	unsigned    thread_id;
	char        pipe_cmd[CMD_MAX + 1] = "",
		    *read_buff;
	FILE        *outfile;
	int         infd;
	ssize_t     bytes,
		    c,
		    my_start,
		    my_end;
	size_t      read_buff_size,
		    read_size;
	struct stat fileinfo;
	
	// Verify that OpenMP has the right thread count
	thread_id = omp_get_thread_num();
	
	// Copy FILE structure to private variables so they can diverge
	infd = open(filename, O_RDONLY);
	
	fstat(infd, &fileinfo);
	read_buff_size = fileinfo.st_blksize;
	printf("Block size = %zu\n", read_buff_size);

	if ( (read_buff = malloc(read_buff_size + 1)) == NULL )
	{
	    fputs("find_start_positions(): Cannot allocate read_buff.\n", stderr);
	    exit(EX_UNAVAILABLE);
	}
	
	// Open a pipe with popen() or a named pipe with fopen()
	snprintf(pipe_cmd, CMD_MAX, "%s > %s", cmd, out_filename);
	if ( (outfile = popen(pipe_cmd, "w")) == NULL )
	{
	    fprintf(stderr, "spawn_processes(): Cannot pipe output: %s\n",
		    pipe_cmd);
	    exit(EX_CANTCREAT);
	}
	
	// Send chars from this thread's section of the file to the pipe
	my_start = start_positions[thread];
	my_end = start_positions[thread + 1];
	lseek(infd, my_start, SEEK_SET);
	
	printf("Thread #%u (%u) sending characters %lu to %lu to %s\n",
		thread, thread_id, my_start, my_end, pipe_cmd);
	for (c = my_start; c < my_end - 1; c += read_size)
	{
	    read_size = MIN(read_buff_size, my_end - c - 1);
	    bytes = read(infd, read_buff, read_size);
	    fwrite(read_buff, read_size, 1, outfile);
	}
	
	pclose(outfile);
	close(infd);
	free(read_buff);
    }
    return EX_OK;
}
