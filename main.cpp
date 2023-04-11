#include <iostream>
#include <string>
#include <memory>
#include "lmdb.h"

using namespace std;
#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

class TableDB;
class Transaction {
    MDB_txn *txn_ = nullptr;
    friend class TableDB;

public:
    int commit(){
        return mdb_txn_commit(txn_);
    }
};

class Iterator {
    MDB_cursor *cursor_ = nullptr;
    MDB_val key_, data_;
    friend class TableDB;
public:
    bool next(){
      return mdb_cursor_get(cursor_, &key_, &data_, MDB_NEXT) == 0;
    }
    MDB_val key(){
        return key_;
    }
    MDB_val value(){
        return data_;
    }
};

class TableDB {
    MDB_env *env = nullptr;
    MDB_dbi dbi = 0;

public:
    TableDB(const string& path, std::size_t size){
        mdb_env_create(&env);
        mdb_env_set_maxreaders(env, 1);
        mdb_env_set_mapsize(env, size);
        mdb_env_open(env, path.data(), MDB_FIXEDMAP |MDB_NOSYNC, 0664);
    }

    int open(Transaction &txn, const string &db_name) {
        return mdb_dbi_open(txn.txn_, db_name.data(), MDB_CREATE, &dbi);
    }

    shared_ptr<Transaction> new_transaction() {
        auto txn = make_shared<Transaction>();
        mdb_txn_begin(env, NULL, 0, &txn->txn_);
        return txn;
    }
    shared_ptr<Iterator> new_iterator(Transaction &txn){
        auto iter = make_shared<Iterator>();
        mdb_cursor_open(txn.txn_, dbi, &iter->cursor_);
        return iter;
    }

    int write(Transaction &txn, const string &key, const string &value) {
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_size = key.size();
        tmp_key.mv_data = (void *) key.data();
        tmp_data.mv_size = value.size();
        tmp_data.mv_data = (void *) value.data();
        return mdb_put(txn.txn_, dbi, &tmp_key, &tmp_data, MDB_NOOVERWRITE);
    }

    int read(Iterator &iter, const string &key, string &out_value){
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_data = (void*)key.data();
        tmp_key.mv_size = key.size();
        auto ret = mdb_cursor_get(iter.cursor_, &tmp_key, &tmp_data, MDB_NEXT);
        out_value.assign((char*)tmp_data.mv_data,tmp_data.mv_size);
        return ret;
    }
};

int main(int argc, char*argv[]){
    // 创建数据库环境对象
    MDB_env* env;
    mdb_env_create(&env);
    mdb_env_set_maxreaders(env, 1);
    mdb_env_set_mapsize(env, 10485760);
    mdb_env_open(env, "./testdb", MDB_FIXEDMAP |MDB_NOSYNC, 0664);
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