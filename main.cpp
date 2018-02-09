#include <qcoreapplication.h>
#include <qdebug.h>
#include "SqlConnectionPool.h"
#include <qthread.h>

bool gExit = false;

void handleSignal(int sig)
{
    gExit = true;
    qApp->exit();
}

void test(SqlConnectionPool & pool)
{
    bool isOk;
    for (int i = 0; i < 3000; ++i)
    {
        SqlResult result = pool.query("SELECT * FROM test", &isOk);
        if (!isOk)
        {
            qDebug() << result.error();
        }
        else
        {
            for (auto &record : result.records())
            {
                QString str;
                for (int i = 0; i < record.count(); ++i)
                    str.append(" ").append(record.field(i).value().toString());
                 qDebug() << str;
            }
        }
    }
}

int main(int argc, char *argv[])
{
    signal(SIGINT, handleSignal);
    QCoreApplication app(argc, argv);

    SqlConnectionPool pool(4, SqlConnection::TYPE_QMYSQL, "test", "root", "123456");


    std::thread thread1([&pool](){test(pool);});
    std::thread thread2([&pool](){test(pool);});
    std::thread thread3([&pool](){test(pool);});

    thread1.join();
    thread2.join();
    thread3.join();

    qDebug("finished");

    int ret = app.exec();

    qDebug("exit");
    return ret;
}
