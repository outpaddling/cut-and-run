/* cut-and-run.c */
int main(int argc, char *argv[]);
long *find_start_positions(int infile, unsigned thread_count);
int spawn_processes(char *filename, char *cmd, char *out_filename, long start_positions[], unsigned thread_count);
