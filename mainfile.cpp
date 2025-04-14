#include "transaction_manager.cpp"

Database db;

void Demo1() {  // Read Uncommited
    TransactionManager t1(db), t2(db);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t1.read(1) << endl;

    t1.write(1, 200);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t1.read(1) << endl;

    t1.rollback();
    cout << "2: " <<  t1.read(1) << endl;
}

void demo2_thread() {
    TransactionManager t1(db, SERIALIZABLE);
    int val = t1.read(1);
    cout << "2: " <<  val << endl;
    sleep(5);
    val = t1.read(1);
    cout << "2: " <<  val << endl;
}

void Demo2() {  // Serializable
    TransactionManager t1(db, SERIALIZABLE);
    thread t = thread(demo2_thread);
    cout << t1.read(1) << endl;
    sleep(5);
    t1.write(1, 200);
    cout << t1.read(1) << endl;
    t1.commit();
    t.join();
    return;
}

void Demo3() {  // Repeatable Read
    TransactionManager t1(db, REPEATABLE_READ), t2(db, REPEATABLE_READ);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t2.read(1) << endl;

    t1.write(1, 200);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t2.read(1) << endl;

    t1.commit();
    cout << "2: " <<  t2.read(1) << endl;
}

// read committed
void Demo4(){
    TransactionManager t1(db, READ_COMMITED), t2(db, READ_COMMITED);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t2.read(1) << endl;

    t1.write(1, 200);

    cout << t1.read(1) << endl;
    cout << "2: " <<  t2.read(1) << endl;
    t1.commit();
    cout << "2: " <<  t2.read(1) << endl;

    t2.write(1, 300);

    
    cout << "2: " <<  t2.read(1) << endl;

    t2.commit();

}

int main() {
    Demo4();

    return 0;
}