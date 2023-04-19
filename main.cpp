#include <iostream>
#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <iomanip>
#include <random>
#include <assert.h>
#include <string.h>
#include <gflags/gflags.h>
#include <omp.h>
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
    enum class NextType {
        Next = MDB_NEXT,
        Dup = MDB_NEXT_DUP,
        NoDup = MDB_NEXT_NODUP
    };
    enum class PrevType {
        Prev = MDB_PREV,
        Dup = MDB_PREV_DUP,
        NoDup = MDB_PREV_NODUP
    };
    enum class FirstType {
        First = MDB_FIRST,
        Dup = MDB_FIRST_DUP,
    };
    enum class LastType {
        Last = MDB_LAST,
        Dup = MDB_LAST_DUP,
    };
    ~Iterator() {
        mdb_cursor_close(cursor_);
    }

    void next(NextType nt = NextType::Next) {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, (MDB_cursor_op)nt) == MDB_SUCCESS);
    }

    void prev(PrevType pt = PrevType::Prev) {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, (MDB_cursor_op)pt) == MDB_SUCCESS);
    }

    void seek_last(LastType lt = LastType::Last) {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, (MDB_cursor_op)lt) == MDB_SUCCESS);
    }

    void seek_first(FirstType ft = FirstType::First) {
        valid_ = ( mdb_cursor_get(cursor_, &key_, &data_, (MDB_cursor_op)ft) == MDB_SUCCESS);
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
    DBEnv(const string& path, std::size_t size,unsigned int flag = (MDB_FIXEDMAP|MDB_NOSYNC)){
        CHECK_MDB(mdb_env_create(&env_));
        CHECK_MDB(mdb_env_set_maxreaders(env_, 100));
        CHECK_MDB(mdb_env_set_mapsize(env_, size));
        CHECK_MDB(mdb_env_set_maxdbs(env_, 40));
        CHECK_MDB(mdb_env_open(env_, path.data(), flag, 0664));
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
    int init(Transaction &txn, const string& db_name,unsigned int flag = MDB_CREATE){
        return mdb_dbi_open(txn.txn_, db_name.data(), flag, &dbi_);
    }
    void close(DBEnv& env){
        mdb_dbi_close(env.env_, dbi_);
    }

    int write(Transaction &txn, Slice key, Slice value, unsigned flag = MDB_NOOVERWRITE) {
        MDB_val tmp_key = key.to_mdb_val();
        MDB_val tmp_data = value.to_mdb_val();
        return mdb_put(txn.txn_, dbi_, &tmp_key, &tmp_data, flag);
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

void print_stats(const char* func_name, int time_cost,size_t counter){
    cout.setf(ios::left);
//    std::cout << std::setw(32) <<func_name  << " timecost:" << time_cost << " μs"<<std::endl;
    std::cout << std::setw(32) <<func_name  << " : " << counter / (double)time_cost * 1000*1000 << " op/s"<< " total:" << counter << " timecost:" << time_cost << " μs"<<std::endl;
}
DEFINE_string(type, "", "");
DEFINE_string(prefix_seek, "1", "param for prefix seek");
DEFINE_string(path, "testdb", "db path");
DEFINE_uint64(count, 1000000, " write count");
DEFINE_uint64(read_count, 1000000, "random read counts");
DEFINE_uint64(db_size, 1, "db size in disk, GB");
DEFINE_bool(print, false, "print result");
void write_test(DBEnv& db_env){
    auto start = std::chrono::high_resolution_clock::now();

    auto txn = db_env.new_transaction();

    DBInstance db_ins;
    db_ins.init(*txn,"db1");
    size_t counter = 0;
    for(std::size_t i = 0; i < FLAGS_count; ++i){
        auto ret = db_ins.write(*txn,to_string(i),to_string(i));
        if(ret == 0){
            ++counter;
        }
    }
    txn->commit();
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    print_stats(__FUNCTION__,time_cost,counter);
    db_ins.close(db_env);
}

void iter_test(DBEnv& db_env){
    auto start = std::chrono::high_resolution_clock::now();

    auto new_txn = db_env.new_transaction(MDB_RDONLY);
    DBInstance db_ins;
    db_ins.init(*new_txn,"db1");


    auto iter = db_ins.new_iterator(*new_txn);
    size_t counter = 0;
    for(iter->seek_first();iter->valid(); iter->next()){
        ++counter;
    }
    new_txn->abort();


    iter.reset();
    new_txn.reset();

    db_ins.close(db_env);
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    print_stats(__FUNCTION__,time_cost,counter);
}
void rand_read_test(DBEnv& db_env){
    //rand read
    auto start = std::chrono::high_resolution_clock::now();

    auto new_txn = db_env.new_transaction(MDB_RDONLY);
    DBInstance db_ins;
    db_ins.init(*new_txn,"db1");


    auto iter = db_ins.new_iterator(*new_txn);
    size_t counter = 0;
    std::random_device rd;  // 将用于为随机数引擎获得种子
    std::mt19937 gen(rd()); // 以播种标准 mersenne_twister_engine
    std::uniform_int_distribution<> dis(0, FLAGS_count);
    for (std::size_t i = 0; i < FLAGS_read_count; ++i) {
        string key = to_string(dis(gen));
        Slice out_value;
        if (db_ins.get(*new_txn, key, out_value)) {
            ++counter;
        }
    }
    new_txn->abort();

    iter.reset();
    new_txn.reset();
    db_ins.close(db_env);
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    print_stats(__FUNCTION__,time_cost,counter);
}

int my_rand(const int & min, const int & max) {
    static thread_local std::mt19937 generator;
    std::uniform_int_distribution<int> distribution(min,max);
    return distribution(generator);
}

void rand_parallel_read_test(DBEnv& db_env){
    //rand read
    auto start = std::chrono::high_resolution_clock::now();

    auto new_txn = db_env.new_transaction(MDB_RDONLY);
    DBInstance db_ins;
    db_ins.init(*new_txn,"db1");


    auto iter = db_ins.new_iterator(*new_txn);
//    size_t counter = 0;
//    std::random_device rd;  // 将用于为随机数引擎获得种子
//    std::mt19937 gen(rd()); // 以播种标准 mersenne_twister_engine
//    std::uniform_int_distribution<> dis(0, FLAGS_count);
#pragma omp parallel for
    for (std::size_t i = 0; i < FLAGS_read_count; ++i) {
        string key = to_string(my_rand(0,FLAGS_count));
        Slice out_value;
        if (db_ins.get(*new_txn, key, out_value)) {
            if(FLAGS_print){
                std::cout << "get key:" << key<<" value :" <<out_value.to_string() << std::endl;
            }
//            ++counter;
        }
    }
    new_txn->abort();

    iter.reset();
    new_txn.reset();
    db_ins.close(db_env);
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    print_stats(__FUNCTION__,time_cost,FLAGS_read_count);
}


void prefix_seek_test(DBEnv& db_env, const string& seek_key){
    //cursor seek
    auto start = std::chrono::high_resolution_clock::now();

    auto new_txn = db_env.new_transaction(MDB_RDONLY);
    DBInstance db_ins;
    db_ins.init(*new_txn,"db1");


    auto iter = db_ins.new_iterator(*new_txn);
    size_t counter = 0;

    for (iter->seek_to(seek_key); iter->valid() &&
                                  iter->key().starts_with(seek_key); iter->next()) {
        if(FLAGS_print){
            std::cout << "value :" << iter->value().to_string_view() << std::endl;
        }
        ++counter;
    }

    iter.reset();
    new_txn.reset();
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    int time_cost = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    print_stats(__FUNCTION__,time_cost,counter);
}

int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    DBEnv db_env(FLAGS_path, (1024*FLAGS_db_size) << 20);

    if(FLAGS_type == "write"){
        write_test(db_env);
    }else if(FLAGS_type == "iter"){
        iter_test(db_env);
    }else if(FLAGS_type == "random_read"){
        rand_read_test(db_env);
    }else if(FLAGS_type == "random_read_parallel"){
        rand_parallel_read_test(db_env);
    }else if(FLAGS_type == "seek"){
        prefix_seek_test(db_env,FLAGS_prefix_seek);
    }

    return 0;
}