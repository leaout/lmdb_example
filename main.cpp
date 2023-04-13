#include <iostream>
#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <random>
#include "lmdb.h"

using namespace std;
#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

#define CHECK_MDB(stmt)                                                        \
    do {                                                                       \
        int ec = (stmt);                                                       \
        if (ec != MDB_SUCCESS) std::cout << "MDB_ERROR: " << mdb_strerror(ec); \
    } while (0)

class DBEnv;
class DBInstance;
class Transaction {
    MDB_txn *txn_ = nullptr;
    friend class DBEnv;
    friend class DBInstance;

public:
    ~Transaction(){

    }
    int commit(){
        return mdb_txn_commit(txn_);
    }
    void abort(){
        mdb_txn_abort(txn_);
    }
};

class Iterator {
    MDB_cursor *cursor_ = nullptr;
    MDB_val key_, data_;
    friend class DBEnv;

    friend class DBInstance;

public:
    ~Iterator() {
        mdb_cursor_close(cursor_);
    }

    bool next() {
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_NEXT) == MDB_SUCCESS;
    }

    bool prev() {
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_PREV) == MDB_SUCCESS;
    }

    bool seek_last() {
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_LAST) == MDB_SUCCESS;
    }

    bool seek_first() {
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_FIRST) == MDB_SUCCESS;
    }

    MDB_val key() {
        return key_;
    }

    MDB_val value() {
        return data_;
    }

};

class DBEnv {
    MDB_env *env_ = nullptr;

    friend class DBInstance;
public:
    DBEnv(const string& path, std::size_t size){
        int rc;
        CHECK_MDB(mdb_env_create(&env_));
        E(mdb_env_set_maxreaders(env_, 100));
        E(mdb_env_set_mapsize(env_, size));
        E(mdb_env_set_maxdbs(env_, 40));
        E(mdb_env_open(env_, path.data(), MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));
    }
    ~DBEnv(){
        mdb_env_close(env_);
    }

    shared_ptr<Transaction> new_transaction(unsigned int flags = 0) {
        auto txn = make_shared<Transaction>();
        CHECK_MDB(mdb_txn_begin(env_, NULL, flags, &txn->txn_));
        return txn;
    }

};

class DBInstance{
    MDB_dbi dbi_ = 0;
public:
    DBInstance() = default;
    int init(Transaction &txn, const string& db_name){
        return mdb_dbi_open(txn.txn_, db_name.data(), MDB_CREATE, &dbi_);
    }
    void close(DBEnv& env){
        mdb_dbi_close(env.env_, dbi_);
    }

    int write(Transaction &txn, const string &key, const string &value) {
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_size = key.size();
        tmp_key.mv_data = (void *) key.data();
        tmp_data.mv_size = value.size();
        tmp_data.mv_data = (void *) value.data();
        return mdb_put(txn.txn_, dbi_, &tmp_key, &tmp_data, MDB_NOOVERWRITE);
    }

    shared_ptr<Iterator> new_iterator(Transaction &txn) {
        auto iter = make_shared<Iterator>();
        mdb_cursor_open(txn.txn_, dbi_, &iter->cursor_);
        return iter;
    }

    int del(Transaction &txn,string& key){
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_data = key.data();
        tmp_key.mv_size = key.size();
        return mdb_del(txn.txn_, dbi_, &tmp_key, &tmp_data);
    }

    bool get(Transaction &txn, string &key, string &out_value) {
        MDB_val tmp_key, tmp_value;
        tmp_key.mv_data = key.data();
        tmp_key.mv_size = key.size();
        auto ret = mdb_get(txn.txn_, dbi_, &tmp_key, &tmp_value);
        CHECK_MDB(ret);
        if(ret == MDB_SUCCESS){
            out_value.assign((char *)tmp_value.mv_data, tmp_value.mv_size);
            return true;
        }

        return false;
    }

};

void print_stats(const char* func_name, int time_cost){
    cout.setf(ios::left);
    std::cout << std::setw(32) <<func_name  << " timecost:" << time_cost << " μs"<<std::endl;
}

int main(int argc, char *argv[]) {

    DBEnv db_env(argv[1], 1024 << 20);
    {
        auto start = std::chrono::high_resolution_clock::now();


        auto txn = db_env.new_transaction();

        DBInstance db_ins;
        db_ins.init(*txn,"db1");

        for(std::size_t i = 0; i < 1000000; ++i){
            auto ret = db_ins.write(*txn,to_string(i),to_string(i));
            if(ret == 0){

            }
        }
        txn->commit();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("write",time_cost);
        db_ins.close(db_env);
    }


    {
        auto start = std::chrono::high_resolution_clock::now();

        auto new_txn = db_env.new_transaction(MDB_RDONLY);
        DBInstance db_ins;
        db_ins.init(*new_txn,"db1");


        auto iter = db_ins.new_iterator(*new_txn);
        int counter = 0;
        while(iter->next()){
            ++counter;
        }
        new_txn->abort();
        std::cout << "iter counter :" << counter << std::endl;

        iter.reset();
        new_txn.reset();

        db_ins.close(db_env);
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("iter",time_cost);
    }

    {
        //rand read
        auto start = std::chrono::high_resolution_clock::now();

        auto new_txn = db_env.new_transaction(MDB_RDONLY);
        DBInstance db_ins;
        db_ins.init(*new_txn,"db1");


        auto iter = db_ins.new_iterator(*new_txn);
        int counter = 0;
        std::random_device rd;  // 将用于为随机数引擎获得种子
        std::mt19937 gen(rd()); // 以播种标准 mersenne_twister_engine
        std::uniform_int_distribution<> dis(0, 999999);
        for(std::size_t i = 0; i < 1000000; ++i){
            string key = to_string(dis(gen));
            string out_value;
            if(db_ins.get(*new_txn,key,out_value)){
                ++counter;
            }
        }

        std::cout << "iter counter :" << counter << std::endl;

        iter.reset();
        new_txn.reset();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("read",time_cost);
    }


    cout << "hello lmdb end!" << endl;
    return 0;
}