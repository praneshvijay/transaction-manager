#include "transaction_manager.hpp"
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <chrono>
#include <fstream>
#include <mutex>
#include <deque>
#include <set>
#include <thread>
#include <unistd.h>

Database db;

void Demo1() {  // Read Uncommited
    cout<<"Isolation Level: Read Uncommited\n";
    TransactionManager t1(db), t2(db);

    t1.read(1);
    t2.read(1);

    t1.write(1, 200);

    t1.read(1);
    t2.read(1);

    t1.rollback();
    t2.read(1);
}

void demo2_thread() {
    TransactionManager t1(db, SERIALIZABLE);
    t1.read(1);
    sleep(5);
    t1.read(1);
}

void Demo2() {  // Serializable
    cout<<"Isolation Level: Serializable\n";
    TransactionManager t1(db, SERIALIZABLE);
    thread t = thread(demo2_thread);

    t1.read(1);
    sleep(5);
    t1.write(1, 200);
    t1.read(1);
    t1.commit();
    t.join();
    return;
}

void Demo3() {  // Repeatable Read
    cout<<"Isolation Level: Repeatable Read\n";
    TransactionManager t1(db, REPEATABLE_READ), t2(db, REPEATABLE_READ);

    t1.read(1);
    t2.read(1);

    t1.write(1, 200);

    t1.read(1);
    t2.read(1);

    t1.commit();
    t2.read(1);
}

// read committed
void Demo4(){
    cout<<"Isolation Level: Read Committed\n";
    TransactionManager t1(db, READ_COMMITED), t2(db, READ_COMMITED);

    t1.read(1);
    t2.read(1);

    t1.write(1, 200);
    
    t1.read(1);
    t2.read(1);

    t1.commit();

    t2.read(1);

    t2.write(1, 300);
    
    t1.read(1);
    
    t2.commit();
}

// ==================== READ UNCOMMITTED DEMONSTRATIONS ====================

void DemoReadUncommitted_DirtyReads() {
    cout << "\n=== READ UNCOMMITTED: Dirty Reads Demonstration ===\n";
    TransactionManager t1(db, READ_UNCOMMITED), t2(db, READ_UNCOMMITED);

    cout << "T1 reads key 1: " << t1.read(1) << endl;
    cout << "T2 reads key 1: " << t2.read(1) << endl;

    t1.write(1, 100);
    cout << "T1 writes 100 to key 1 (uncommitted)" << endl;

    cout << "T1 reads key 1: " << t1.read(1) << endl;
    cout << "T2 reads key 1: " << t2.read(1) << " (DIRTY READ!)" << endl;

    t1.rollback();
    cout << "T1 rolls back its changes" << endl;

    cout << "T2 reads key 1: " << t2.read(1) << endl;
}

void DemoReadUncommitted_NonRepeatableReads() {
    cout << "\n=== READ UNCOMMITTED: Non-Repeatable Reads Demonstration ===\n";
    TransactionManager t1(db, READ_UNCOMMITED), t2(db, READ_UNCOMMITED);

    cout << "T1 reads key 2: " << t1.read(2) << endl;

    t2.write(2, 200);
    cout << "T2 writes 200 to key 2" << endl;
    t2.commit();
    cout << "T2 commits" << endl;

    cout << "T1 reads key 2 again: " << t1.read(2) << " (NON-REPEATABLE READ!)" << endl;
}

// ==================== READ COMMITTED DEMONSTRATIONS ====================

void DemoReadCommitted_DirtyReads() {
    cout << "\n=== READ COMMITTED: Dirty Reads Prevention ===\n";
    TransactionManager t1(db, READ_COMMITED), t2(db, READ_COMMITED);

    cout << "T1 reads key 3: " << t1.read(3) << endl;
    cout << "T2 reads key 3: " << t2.read(3) << endl;

    t1.write(3, 300);
    cout << "T1 writes 300 to key 3 (uncommitted)" << endl;

    cout << "T1 reads key 3: " << t1.read(3) << " (can see own uncommitted changes)" << endl;
    cout << "T2 reads key 3: " << t2.read(3) << " (cannot see T1's uncommitted changes)" << endl;

    t1.commit();
    cout << "T1 commits" << endl;

    cout << "T2 reads key 3 again: " << t2.read(3) << " (now sees committed changes)" << endl;
}

void DemoReadCommitted_NonRepeatableReads() {
    cout << "\n=== READ COMMITTED: Non-Repeatable Reads Demonstration ===\n";
    TransactionManager t1(db, READ_COMMITED), t2(db, READ_COMMITED);

    cout << "T1 reads key 4: " << t1.read(4) << endl;

    t2.write(4, 400);
    cout << "T2 writes 400 to key 4" << endl;
    t2.commit();
    cout << "T2 commits" << endl;

    cout << "T1 reads key 4 again: " << t1.read(4) << " (NON-REPEATABLE READ!)" << endl;
}

// ==================== REPEATABLE READ DEMONSTRATIONS ====================

void DemoRepeatableRead_NonRepeatableReadsPrevention() {
    cout << "\n=== REPEATABLE READ: Non-Repeatable Reads Prevention ===\n";
    TransactionManager t1(db, REPEATABLE_READ), t2(db, REPEATABLE_READ);

    cout << "T1 reads key 5: " << t1.read(5) << endl;

    t2.write(5, 500);
    cout << "T2 writes 500 to key 5" << endl;
    t2.commit();
    cout << "T2 commits" << endl;

    cout << "T1 reads key 5 again: " << t1.read(5) << " (still sees original value)" << endl;

    t1.commit();
    cout << "T1 commits" << endl;
    
    // New transaction will see the updated value
    TransactionManager t3(db, REPEATABLE_READ);
    cout << "T3 (new transaction) reads key 5: " << t3.read(5) << endl;
}

// ==================== SERIALIZABLE DEMONSTRATIONS ====================

void serializable_thread() {
    sleep(1); // Slight delay to ensure proper output ordering
    TransactionManager t2(db, SERIALIZABLE);
    cout << "T2 started" << endl;
    
    cout << "T2 reads key 6: " << t2.read(6) << endl;
    t2.write(6, 650);
    cout << "T2 writes 650 to key 6" << endl;
    t2.commit();
    cout << "T2 committed" << endl;
}

void DemoSerializable() {
    cout << "\n=== SERIALIZABLE: Complete Isolation Demonstration ===\n";
    
    TransactionManager t1(db, SERIALIZABLE);
    cout << "T1 started" << endl;
    
    thread t = thread(serializable_thread);

    cout << "T1 reads key 6: " << t1.read(6) << endl;
    t1.write(6, 600);
    cout << "T1 writes 600 to key 6" << endl;
    t1.commit();
    cout << "T1 committed" << endl;
    
    t.join();
    
    // Verify final value
    TransactionManager t3(db, READ_COMMITED);
    cout << "T3 (new transaction) reads key 6: " << t3.read(6) << endl;
}

void GarbageCollector() {
    while(1){
        sleep(10);
        db.garbage_collector();
    }
}


int main() {
    thread gc = thread(GarbageCollector);
    gc.detach();
    // Demo4();
    DemoReadUncommitted_DirtyReads();
    DemoReadUncommitted_NonRepeatableReads();
    
    DemoReadCommitted_DirtyReads();
    DemoReadCommitted_NonRepeatableReads();
    
    DemoRepeatableRead_NonRepeatableReadsPrevention();
    
    DemoSerializable();
    cout<<endl;
    
    return 0;
}