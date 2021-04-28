/* cut-and-run.c */
int main(int argc, char *argv[]);
long *find_start_positions(int infd, unsigned thread_count);
int spawn_processes(const char *filename, const char *cmd, const char *out_filename, const char *extension, const long start_positions[], unsigned thread_count);
void usage(char *argv[]);
