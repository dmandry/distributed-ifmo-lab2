/**
 * @file     main.c
 * @Author   Andrey Dmitriev (dmandry92@gmail.com)
 * @date     September, 2015
 */

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>
#include <string.h>
#include <unistd.h>

#include "banking.h"
#include "common.h"
#include "ipc.h"
#include "load.h"
#include "pa2345.h"
#include "process_transmission.h"

extern int started_len;
extern int done_len;
extern char free_payload[MAX_PAYLOAD_LEN];

balance_t cash[11];
extern AllHistory *allhist;

FILE *eventlog;

void child(int8_t num_processes, local_id id)
{
	reset_balance_state(cash[id]);
	reset_balance_history(id);
	timestamp_t time = get_physical_time();
	printf(log_started_fmt,
	       time,
	       id,
	       getpid(),
	       getppid(),
	       get_balance());
	fprintf(eventlog,
	        log_started_fmt,
	        time,
	        id,
	        getpid(),
	        getppid(),
	        get_balance());
	process_send_multicast(id, STARTED);
	process_recieve_all(num_processes, id, STARTED);
	time = get_physical_time();
	printf(log_received_all_started_fmt, time, id);
	fprintf(eventlog, log_received_all_started_fmt, time, id);
	process_load(id);
	time = get_physical_time();
	printf(log_done_fmt, time, id, get_balance());
	fprintf(eventlog, log_done_fmt, time, id, get_balance());
	process_send_multicast(id, DONE);
	process_recieve_all(num_processes, id, DONE);
	time = get_physical_time();
	printf(log_received_all_done_fmt, time, id);
	fprintf(eventlog, log_received_all_done_fmt, time, id);
	send_balance_history();
}

// Создание дочерних процессов
void create_childs(int8_t num_processes)
{
	local_id i;
	for (i=0; i<num_processes; i++)
	{
		int16_t child_pid;
		child_pid = fork();
		if (child_pid == -1)
		{
			fclose(eventlog);
			perror("fork");
			exit(EXIT_FAILURE);
		}
		if (child_pid == 0)
		{
		    close_unused_pipes(num_processes, i+1);
		    child(num_processes, i+1);
			close_used_pipes(num_processes, i+1);
			fclose(eventlog);
			exit(EXIT_SUCCESS);
		}
	}
}

// Ожидание завершения дочерних процессов и
// получение сообщений без обработки (для предотвращения переполнения)
void wait_for_childs(int8_t num_processes)
{
	while (num_processes > 0)
	{
		int16_t w;
		w = waitpid(-1, NULL, WEXITED || WNOHANG);
		if (w > 0)
		    num_processes--;
		if (w == -1)
		{
			fclose(eventlog);
			perror("wait");
			exit(EXIT_FAILURE);
		}
	}
}

void parent(int8_t num_processes, FILE *pipelog)
{
	create_all_history(num_processes);
	create_pipe_topology(num_processes, pipelog);
	create_childs(num_processes);
	close_unused_pipes(num_processes, PARENT_ID);
	process_recieve_all(num_processes, PARENT_ID, STARTED);
	bank_robbery(NULL, num_processes);
	process_send_multicast(PARENT_ID, STOP);
	process_recieve_all(num_processes, PARENT_ID, DONE);
	process_recieve_all(num_processes, PARENT_ID, BALANCE_HISTORY);
    print_history(allhist);
    wait_for_childs(num_processes);
	close_used_pipes(num_processes, PARENT_ID);
}

// Обработка ключей и запись параметров
void set_opts(int argc, char *argv[], char opt, int8_t *num_processes)
{
    switch (opt)
	{
		case 'p':
		    if ((*num_processes = atoi(argv[optind])) < 0)
		    {
		        fprintf(stderr, "Usage: %s [-p numofprocesses]\n", argv[0]);
                exit(EXIT_FAILURE);
			}
			break;
	}
}

int main(int argc, char *argv[])
{
	int8_t num_processes;
	local_id i;
	const char *opts = "p";
	char opt;
	FILE *pipelog;
	// Обработка параметров запуска
	if(argc < 3) {
        fprintf(stderr, "Usage: %s [-p numofprocesses] [balance_1 .. balance_n]\n", argv[0]);
        exit(EXIT_FAILURE);
    }
	while ((opt = getopt(argc,argv,opts)) != -1)
	{
		set_opts(argc, argv, opt, &num_processes);
	}
	for (i=0; i<num_processes; i++)
	{
		
		cash[i+1] = atoi(argv[i+3]);
	}
	// Открытие файлов логирования
	if (!(pipelog = fopen(pipes_log, "w")) || !(eventlog = fopen(events_log, "a")))
	{
		perror("fopen");
		exit(EXIT_FAILURE);
	}
	// Узнаем размер посылаемых сообщений
	sprintf(free_payload, log_started_fmt, get_physical_time(), PARENT_ID, getpid(), getppid(), 99);
	started_len = strlen(free_payload);
	sprintf(free_payload, log_done_fmt, get_physical_time(), PARENT_ID, 99);
	done_len = strlen(free_payload);
    // Выполнение родительского процесса
    parent(num_processes, pipelog);
	fclose(eventlog);
	return 0;
}

