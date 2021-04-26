/* cut-and-run.c */
int main(int argc, char *argv[]);
long *find_start_positions(FILE *infile, unsigned thread_count);
int spawn_processes(char *filename, long start_positions[], unsigned thread_count);
