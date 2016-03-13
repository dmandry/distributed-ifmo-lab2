/**
 * @file     load.h
 * @Author   Andrey Dmitriev (dmandry92@gmail.com)
 * @date     September, 2015
 * @brief    Buisness-process functionality
 */

#include "load.h"

#include <malloc.h>
#include <stdint.h>
#include <string.h>

#include "banking.h"
#include "pa2345.h"
#include "process_transmission.h"

extern FILE *eventlog;

BalanceState balstate;
BalanceHistory balhist;
AllHistory *allhist = NULL;

int stop_flag = 0;
int ack_flag = 1;

balance_t get_balance()
{
	return balstate.s_balance;
}

void create_all_history(int8_t num_processes)
{
	allhist = malloc(sizeof(uint8_t)+num_processes*sizeof(BalanceHistory));
	allhist->s_history_len = num_processes;
}

void reset_balance_history(local_id id)
{
	balhist.s_id = id;
	balhist.s_history_len = 1;
	balhist.s_history[0] = balstate;
}

void reset_balance_state(balance_t bal)
{
	balstate.s_balance = bal;
	balstate.s_time = get_physical_time();
	balstate.s_balance_pending_in = 0;
}

void send_balance_history()
{
	process_send(balhist.s_id, PARENT_ID, BALANCE_HISTORY, NULL, &balhist);
}

void process_msg_ack(Message *msg)
{
	ack_flag = 1;
}

void process_balance_history(Message *msg)
{
	memcpy(&balhist, msg->s_payload, msg->s_header.s_payload_len);
	allhist->s_history[balhist.s_id - 1] = balhist;
}

void add_transaction(balance_t ammount)
{
	timestamp_t time = get_physical_time();
	timestamp_t i;
	for (i=balstate.s_time + 1; i < time; i++)
	{
		balstate.s_balance = balstate.s_balance;
	    balstate.s_time = i;
	    balhist.s_history[balhist.s_history_len] = balstate;
	    balhist.s_history_len++;
	}
	balstate.s_balance = balstate.s_balance + ammount;
	balstate.s_time = time;
	balhist.s_history[balhist.s_history_len] = balstate;
	balhist.s_history_len++;
}

void process_msg_transfer(Message *msg, local_id id)
{
    TransferOrder *order = malloc(sizeof(TransferOrder));
	timestamp_t time = get_physical_time();
	order->s_src = ((TransferOrder *)(msg->s_payload))->s_src;
	order->s_dst = ((TransferOrder *)(msg->s_payload))->s_dst;
	order->s_amount = ((TransferOrder *)(msg->s_payload))->s_amount;
	if (id == order->s_src)
	{
		if (stop_flag == 0)
		{
			add_transaction(-order->s_amount);
		    process_send(id, order->s_dst, TRANSFER, order, NULL);
	        printf(log_transfer_out_fmt,
	               time,
	               id,
	               order->s_amount,
	               order->s_dst);
	        fprintf(eventlog,
	                log_transfer_out_fmt,
	                time,
	                id,
	                order->s_amount,
	                order->s_dst);
	    }
	}
	if (id == order->s_dst)
	{
		add_transaction(order->s_amount);
		process_send(id, PARENT_ID, ACK, NULL, NULL);
	    printf(log_transfer_in_fmt,
	           time,
	           id,
	           order->s_amount,
	           order->s_src);
	    fprintf(eventlog,
	            log_transfer_in_fmt,
	            time,
	            id,
	            order->s_amount,
	            order->s_src);
	}
	free(order);
}

void process_msg_stop(Message *msg)
{
	stop_flag = 1;
}

void transfer(void * parent_data, local_id src, local_id dst,
              balance_t amount)
{
    TransferOrder *order = malloc(sizeof(TransferOrder));
	order->s_src = src;
	order->s_dst = dst;
	order->s_amount = amount;
	process_send(PARENT_ID, src, TRANSFER, order, NULL);
	ack_flag = 0;
	while(!ack_flag)
	{
		process_recieve_any(PARENT_ID);
	}
	free(order);
}

void load(local_id id)
{
	while (!stop_flag)
	{
	    process_recieve_any(id);
	}
	add_transaction(0);
}
