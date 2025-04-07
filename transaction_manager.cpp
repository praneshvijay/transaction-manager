#include "transaction_manager.hpp"
#include <unistd.h>
#include <thread>

/*
TO be implemented -
    === Important - research pending ===
    -> Read Commited - (Deadlock detection)
    -> Repeatable Read - (Optimized snapshot creation, Commit)

    === Small tasks ===
    -> Rollback
*/

TransactionManager::TransactionManager(Database& db, int il = READ_UNCOMMITED): db(db),  isolation_level(il) {
    if (il == REPEATABLE_READ) {
        snapshot = db;
    } else if (il == SERIALIZABLE) {
        db.lock_mutex_s();
    }
}

template<typename... Args>
int TransactionManager::read(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            return db.read(args...);
            break;
        case READ_COMMITED:
            return db.read_commit(args...);
            break;
        case SERIALIZABLE:
            return db.read(args...);
            break;
        case REPEATABLE_READ:
            return snapshot.read(args...);
            break;
    }

    return 0;
}

template<typename... Args>
void TransactionManager::write(Args... args) {
    switch(isolation_level) {
        case READ_UNCOMMITED:
            db.write(args...);
            break;
        case READ_COMMITED:
            db.write(args...);
            break;
        case SERIALIZABLE:
            db.write(args...);
            break;
        case REPEATABLE_READ:
            snapshot.write(args...);
            break;
    }
}

void TransactionManager::commit() {
    if (isolation_level == SERIALIZABLE) {
        db.unlock_mutex_s();
    }
}

Database db;

void Demo1() {  // Read Uncommited
    TransactionManager t1(db), t2(db);

    cout << t1.read("key1") << endl;
    cout << "2: " <<  t1.read("key1") << endl;

    t1.write("key1", 200, 1);

    cout << t1.read("key1") << endl;
    cout << "2: " <<  t1.read("key1") << endl;
}

void demo2_thread() {
    TransactionManager t1(db, SERIALIZABLE);
    cout << "2: " <<  t1.read("key1") << endl;
    sleep(5);
    cout << "2: " << t1.read("key1") << endl;
}

void Demo2() {  // Serializable
    TransactionManager t1(db, SERIALIZABLE);
    thread t = thread(demo2_thread);
    cout << t1.read("key1") << endl;
    sleep(5);
    t1.write("key1", 200, 1);
    cout << t1.read("key1") << endl;
    t1.commit();
    t.join();
    return;
}

void Demo3() {  // Repeatable Read
    TransactionManager t1(db, REPEATABLE_READ), t2(db, REPEATABLE_READ);

    cout << t1.read("key1") << endl;
    cout << "2: " <<  t2.read("key1") << endl;

    t1.write("key1", 200, 1);

    cout << t1.read("key1") << endl;
    cout << "2: " <<  t2.read("key1") << endl;
}

int main() {
    Demo3();

    return 0;
}