// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

#ifndef ERLFDB_RESOURCES_H
#define ERLFDB_RESOURCES_H

#include <stdbool.h>

#include "erl_nif.h"
#include "erl_driver.h"
#include "fdb.h"


extern ErlNifResourceType* ErlFDBFutureRes;
extern ErlNifResourceType* ErlFDBDatabaseRes;
extern ErlNifResourceType* ErlFDBTransactionRes;
extern ErlNifResourceType* ErlFDBTenantRes;


typedef struct _ErlFDBFuture ErlFDBFuture;
typedef ERL_NIF_TERM (*ErlFDBFutureGetter)(ErlNifEnv*, ErlFDBFuture*);
struct _ErlFDBFuture
{

    FDBFuture* future;
    ErlFDBFutureGetter fgetter;
    ErlNifPid pid;
    ErlNifEnv* msg_env;
    ERL_NIF_TERM msg_ref;
    ErlDrvTSDKey future_proc_env_key;
    bool cancelled;
    ErlNifMutex* cancel_lock;
};


typedef struct _ErlFDBDatabase
{
    FDBDatabase* database;
} ErlFDBDatabase;


typedef struct _ErlFDBTransaction
{
    FDBTransaction* transaction;
    ERL_NIF_TERM owner;
    unsigned int txid;
    bool read_only;
    bool writes_allowed;
    bool has_watches;
} ErlFDBTransaction;

typedef struct _ErlFDBTenant
{
    FDBTenant* tenant;
} ErlFDBTenant;

int erlfdb_init_resources(ErlNifEnv* env);
void erlfdb_future_dtor(ErlNifEnv* env, void* obj);
void erlfdb_database_dtor(ErlNifEnv* env, void* obj);
void erlfdb_transaction_dtor(ErlNifEnv* env, void* obj);
void erlfdb_tenant_dtor(ErlNifEnv* env, void* obj);

int erlfdb_transaction_is_owner(ErlNifEnv* env, ErlFDBTransaction* t);


#endif // Included resources.h
