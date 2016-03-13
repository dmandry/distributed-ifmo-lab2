/**
 * @file     load.h
 * @Author   Andrey Dmitriev (dmandry92@gmail.com)
 * @date     September, 2015
 * @brief    Library for buisness-process functionality
 */

#ifndef __IFMO_DISTRIBUTED_CLASS_LOAD__H
#define __IFMO_DISTRIBUTED_CLASS_LOAD__H

#include <stdint.h>

#include "banking.h"
#include "ipc.h"

//------------------------------------------------------------------------------

/** Performs workload after all syncronisation procedures.
 *
 * @param id            Id of the process
 *
 */
void load(local_id id);

//------------------------------------------------------------------------------

/** Processes transfer message.
 *
 * @param msg           Message
 * @param id            Id of the process
 *
 */
void process_msg_transfer(Message *msg, local_id id);

//------------------------------------------------------------------------------

/** Processes stop message.
 *
 * @param msg           Message
 *
 */
void process_msg_stop(Message *msg);

//------------------------------------------------------------------------------

/** Processes ack message.
 *
 * @param msg           Message
 *
 */
void process_msg_ack(Message *msg);

//------------------------------------------------------------------------------

/** Processes balance history message.
 *
 * @param msg           Message
 *
 */
void process_balance_history(Message *msg);

//------------------------------------------------------------------------------

/** Resets balance history with balance state.
 *
 * @param num_processes Num of childs
 *
 */
void create_all_history(int8_t num_processes);

//------------------------------------------------------------------------------

/** Resets balance history with balance state.
 *
 * @param id            Id of the process
 *
 */
void reset_balance_history(local_id id);

//------------------------------------------------------------------------------

/** Resets balance state.
 *
 * @param bal            Startup balance
 *
 */
void reset_balance_state(balance_t bal);

//------------------------------------------------------------------------------

/** Sends balance history to a client */
void send_balance_history();

//------------------------------------------------------------------------------

/** Get current balance
 *
 * @retrun current balance
 */
balance_t get_balance();

//------------------------------------------------------------------------------

#endif // __IFMO_DISTRIBUTED_CLASS_LOAD__H
