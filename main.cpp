#include <iostream>
#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <iomanip>
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
    friend class TableDB;
public:
    ~Iterator(){

    }
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
    MDB_env *env_ = nullptr;
    MDB_dbi dbi_ = 0;

public:
    TableDB(const string& path, std::size_t size){
        mdb_env_create(&env_);
        mdb_env_set_maxreaders(env_, 100);
        mdb_env_set_mapsize(env_, size);
        mdb_env_set_maxdbs(env_, 40);
        mdb_env_open(env_, path.data(), MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664);
    }
    ~TableDB(){
        mdb_dbi_close(env_, dbi_);
        mdb_env_close(env_);
    }
    int open(Transaction &txn, const string &db_name) {
        return mdb_dbi_open(txn.txn_, db_name.data(), MDB_CREATE, &dbi_);
    }

    shared_ptr<Transaction> new_transaction() {
        auto txn = make_shared<Transaction>();
        mdb_txn_begin(env_, NULL, 0, &txn->txn_);
        return txn;
    }
    shared_ptr<Iterator> new_iterator(Transaction &txn){
        auto iter = make_shared<Iterator>();
        mdb_cursor_open(txn.txn_, dbi_, &iter->cursor_);
        return iter;
    }

    int write(Transaction &txn, const string &key, const string &value) {
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_size = key.size();
        tmp_key.mv_data = (void *) key.data();
        tmp_data.mv_size = value.size();
        tmp_data.mv_data = (void *) value.data();
        return mdb_put(txn.txn_, dbi_, &tmp_key, &tmp_data, MDB_NOOVERWRITE);
    }

    int read(Iterator &iter, const string &key, string &out_value){
        MDB_val tmp_key, tmp_data;
        tmp_key.mv_data = (void*)key.data();
        tmp_key.mv_size = key.size();
        auto ret = mdb_cursor_get(iter.cursor_, &tmp_key, &tmp_data, MDB_NEXT);
        if(ret == 0){
            out_value.assign((char*)tmp_data.mv_data,tmp_data.mv_size);
        }

        return ret;
    }
};
void print_stats(const char* func_name, int time_cost){
    cout.setf(ios::left);
    std::cout << std::setw(32) <<func_name  << " timecost:" << time_cost << " μs"<<std::endl;
}

int main(int argc, char *argv[]) {
    int rc = 0;
    {
        auto start = std::chrono::high_resolution_clock::now();
        TableDB table_db(argv[1], 1024 << 20);

        auto txn = table_db.new_transaction();
        E(table_db.open(*txn,"db1"));

        for(std::size_t i = 0; i < 1000000; ++i){
            auto ret = table_db.write(*txn,to_string(i),to_string(i));
            if(ret == 0){

            }
        }
        txn->commit();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("write",time_cost);
    }


    {
        auto start = std::chrono::high_resolution_clock::now();
        TableDB table_db(argv[1], 1024 << 20);
        auto new_txn = table_db.new_transaction();
        E(table_db.open(*new_txn,"db1"));
        auto iter = table_db.new_iterator(*new_txn);

        for(std::size_t i = 0; i < 1000000; ++i){
            string value;
            if(table_db.read(*iter,to_string(i),value) == 0){
//                std::cout << value << std::endl;
            }
        }
        new_txn.reset();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("read",time_cost);
    }


    cout << "hello lmdb end!" << endl;
    return 0;
}