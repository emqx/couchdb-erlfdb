% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(erlfdb_07_tenant_test).

-include_lib("eunit/include/eunit.hrl").

tenant_management_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant = gen(10),
    ?assertEqual(ok, erlfdb_tenant:create_tenant(Db, Tenant)),
    ?assertEqual({error, tenant_already_exists}, erlfdb_tenant:create_tenant(Db, Tenant)),
    ?assertEqual(ok, erlfdb_tenant:delete_tenant(Db, Tenant)),
    ?assertEqual({error, tenant_not_found}, erlfdb_tenant:delete_tenant(Db, Tenant)).

tenant_list_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant1 = gen(10),
    Tenant2 = gen(10),
    ?assertEqual(ok, erlfdb_tenant:create_tenant(Db, Tenant1)),
    ?assertEqual(ok, erlfdb_tenant:create_tenant(Db, Tenant2)),
    {Tenants, _} = lists:unzip(erlfdb_tenant:list_tenants(Db, <<>>, <<16#FF>>, 99999999)),
    ?assert(lists:member(Tenant1, Tenants)),
    ?assert(lists:member(Tenant2, Tenants)).

tenant_get_id_test() ->
    Db = erlfdb_util:get_test_db(),
    Tenant = gen(10),
    ?assertEqual(ok, erlfdb_tenant:create_tenant(Db, Tenant)),
    Tn = erlfdb_tenant:open_tenant(Db, Tenant),
    ?assert(is_integer(erlfdb:wait(erlfdb_tenant:get_id(Tn)))).

gen(N) ->
    << <<(gen_char())>> || _ <- lists:seq(1, N) >>.

gen_char() ->
    $a - 1 + rand:uniform($z - $a + 1).

