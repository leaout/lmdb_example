#include <iostream>
#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <random>
#include <assert.h>
#include <string.h>
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


class Slice {
public:

    Slice() : data_(""), size_(0) {}

    Slice(const char *d, size_t n) : data_(d), size_(n) {}


    Slice(const std::string &s) : data_(s.data()), size_(s.size()) {}

#ifdef __cpp_lib_string_view

    Slice(std::string_view sv) : data_(sv.data()), size_(sv.size()) {}

#endif

    Slice(const char *s) : data_(s) { size_ = (s == nullptr) ? 0 : strlen(s); }

    Slice(MDB_val &value) : data_((char *) value.mv_data), size_(value.mv_size) {}


    const char *data() const { return data_; }

    size_t size() const { return size_; }

    bool empty() const { return size_ == 0; }

    bool starts_with(const Slice &x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

    bool ends_with(const Slice &x) const {
        return ((size_ >= x.size_) &&
                (memcmp(data_ + size_ - x.size_, x.data_, x.size_) == 0));
    }

    std::string to_string(bool hex = false) const {
        std::string result(data_, size_);
        return result;
    }

#ifdef __cpp_lib_string_view

    std::string_view to_string_view() const {
        return std::string_view(data_, size_);
    }

#endif

    MDB_val to_mdb_val() {
        MDB_val ret;
        ret.mv_data = (void *) data_;
        ret.mv_size = size_;
        return ret;
    }

    const char *data_;
    size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

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
    bool valid_ = false;
    friend class DBEnv;

    friend class DBInstance;

public:
    ~Iterator() {
        mdb_cursor_close(cursor_);
    }

    void next() {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, MDB_NEXT) == MDB_SUCCESS);
    }

    void prev() {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, MDB_PREV) == MDB_SUCCESS);
    }

    void seek_last() {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, MDB_LAST) == MDB_SUCCESS);
    }

    void seek_first() {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, MDB_FIRST) == MDB_SUCCESS);
    }
    bool has_key(Slice key){
        key_ = key.to_mdb_val();
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_SET) == MDB_SUCCESS;
    }
    bool get(Slice key){
        key_ = key.to_mdb_val();
        return mdb_cursor_get(cursor_, &key_, &data_, MDB_SET_KEY) == MDB_SUCCESS;
    }
    void seek_to(Slice key){
        key_ = key.to_mdb_val();
        valid_ = (mdb_cursor_get(cursor_, &key_, &data_, MDB_SET_RANGE) == MDB_SUCCESS);
    }

    Slice key() {
        return key_;
    }

    Slice value() {
        return data_;
    }
    bool valid(){
        return valid_;
    }
};

class DBEnv {
    MDB_env *env_ = nullptr;

    friend class DBInstance;
public:
    DBEnv(const string& path, std::size_t size){
        CHECK_MDB(mdb_env_create(&env_));
        CHECK_MDB(mdb_env_set_maxreaders(env_, 100));
        CHECK_MDB(mdb_env_set_mapsize(env_, size));
        CHECK_MDB(mdb_env_set_maxdbs(env_, 40));
        CHECK_MDB(mdb_env_open(env_, path.data(), MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));
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

    int write(Transaction &txn, Slice key, Slice value) {
        MDB_val tmp_key = key.to_mdb_val();
        MDB_val tmp_data = value.to_mdb_val();
        return mdb_put(txn.txn_, dbi_, &tmp_key, &tmp_data, MDB_NOOVERWRITE);
    }

    shared_ptr<Iterator> new_iterator(Transaction &txn) {
        auto iter = make_shared<Iterator>();
        mdb_cursor_open(txn.txn_, dbi_, &iter->cursor_);
        return iter;
    }

    int del(Transaction &txn, Slice &key) {
        MDB_val tmp_key, tmp_data;
        tmp_key = key.to_mdb_val();
        return mdb_del(txn.txn_, dbi_, &tmp_key, &tmp_data);
    }

    bool get(Transaction &txn, Slice key, Slice &out_value) {
        MDB_val tmp_value;
        MDB_val tmp_key = key.to_mdb_val();
        auto ret = mdb_get(txn.txn_, dbi_, &tmp_key, &tmp_value);
        CHECK_MDB(ret);
        if (ret == MDB_SUCCESS) {
            out_value = tmp_value;
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
        for(iter->seek_first();iter->valid(); iter->next()){
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
        for (std::size_t i = 0; i < 1000000; ++i) {
            string key = to_string(dis(gen));
            Slice out_value;
            if (db_ins.get(*new_txn, key, out_value)) {
                ++counter;
            }
        }
        new_txn->abort();
        std::cout << "iter counter :" << counter << std::endl;

        iter.reset();
        new_txn.reset();
        db_ins.close(db_env);
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("rand read",time_cost);
    }

    {
        //cursor seek
        auto start = std::chrono::high_resolution_clock::now();

        auto new_txn = db_env.new_transaction(MDB_RDONLY);
        DBInstance db_ins;
        db_ins.init(*new_txn,"db1");


        auto iter = db_ins.new_iterator(*new_txn);
        int counter = 0;
        string seek_key = "1";
        for (iter->seek_to(seek_key); iter->valid() &&
                                      iter->key().starts_with(seek_key); iter->next()) {
//            std::cout << "value :" << iter->value().to_string_view() << std::endl;
            ++counter;
        }

        std::cout << "iter counter :" << counter << std::endl;

        iter.reset();
        new_txn.reset();
        auto elapsed = std::chrono::high_resolution_clock::now() - start;
        int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        print_stats("cursor",time_cost);
    }


    cout << "hello lmdb end!" << endl;
    return 0;
}