#include <iostream>
#include <string>
#include "lmdb.h"

using namespace std;
#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

int main(int argc, char*argv[]){
    // 创建数据库环境对象
    MDB_env* env;
    mdb_env_create(&env);
    mdb_env_set_maxreaders(env, 1);
    mdb_env_set_mapsize(env, 10485760);
    mdb_env_open(env, "./testdb", MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664);
    MDB_txn* txn;
    mdb_txn_begin(env, NULL, 0, &txn);
    MDB_dbi dbi;
    mdb_dbi_open(txn, NULL, 0, &dbi);

    // 存储数据
    string key = "hello";
    string value = "world";
    MDB_val k = { key.size(), (void*)key.data() };
    MDB_val v = { value.size(), (void*)value.data() };
    mdb_put(txn, dbi, &k, &v, 0);

    // 提交事务
    mdb_txn_commit(txn);

    // 关闭数据库表和环境对象
    mdb_dbi_close(env, dbi);
    mdb_env_close(env);

    cout << "hello lmdb !!" <<endl;
    return 0;
}