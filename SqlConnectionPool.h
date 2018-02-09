#ifndef SQLCONNECTIONPOOL_H
#define SQLCONNECTIONPOOL_H

#include <qsqldatabase.h>
#include <qsqlquery.h>
#include <qvariant.h>
#include <mutex>
#include <memory>
#include <thread>
#include <QEventLoop>
#include <qsqlrecord.h>
#include <qsqlfield.h>
#include <atomic>
#include <qdatetime.h>
#include <qvector.h>
#include <QSqlError>

typedef QSqlRecord SqlRecord;
typedef QVector<SqlRecord> SqlRecords;

struct SqlResultPrivate
{
    QSqlError error;
    SqlRecords records;
    QVariant lastInsertId;
    int numRowsAffected;
};

class SqlResult
{
public:
    SqlResult(std::shared_ptr<SqlResultPrivate> result);
    ~SqlResult();

    const QSqlError& error();
    const SqlRecords& records();
    const QVariant& lastInsertId();
    int numRowsAffected();

private:
    std::shared_ptr<SqlResultPrivate> m_result;
};

class SqlConnectionHandler : public QObject
{
    Q_OBJECT

public:
    SqlConnectionHandler(const QString &type, const QString &databaseName = "",
        const QString &userName = "root", const QString &passowrd = "",
        const QString &host = "127.0.0.1",  int port = 3306);
    ~SqlConnectionHandler();

    void setAutoOpenInterval(unsigned ms);

private slots:

    void doQeuryWithArgsMap(const QString &sql, const QMap<QString, QVariant> *args, SqlResultPrivate &result, bool &isOk);
    void doQeuryWithArgsList(const QString &sql, const QVector<QVariant> *args, SqlResultPrivate &result, bool &isOk);

private:
    bool open();
    void initHandler();
    void releaseHandler();
    bool doQeury(QSqlQuery &query, SqlResultPrivate &result, bool &isOk);
    bool checkConnection(QSqlError &err);

    template <typename T>
    void doQuery(const QString &sql, const T *args, SqlResultPrivate &result, bool &isOk)
    {
        isOk = checkConnection(result.error);
        if(!isOk)
            return;

        bool b;
        {
            QSqlQuery query(*m_handler);
            query.prepare(sql);

            prepareArgs<T>(query, args);
            b = doQeury(query, result, isOk);
        }
        if(!b)
            releaseHandler();
    }

    template <typename T>
    void prepareArgs(QSqlQuery &query, const T *args)
    {
        prepareArgs(query, args);
    }

    void prepareArgs(QSqlQuery &query, const QMap<QString, QVariant> *args);
    void prepareArgs(QSqlQuery &query, const QVector<QVariant> *args);

private:
    QSqlDatabase *m_handler;
    QDateTime m_lastOpenTime;
    unsigned m_autoOpenInterval;

    QString m_type;
    QString m_databaseName;
    QString m_userName;
    QString m_passowrd;
    QString m_host;
    int m_port;

    QString m_testSql;

    static std::mutex s_mutex;
};

class SqlConnection : public QObject
{
    Q_OBJECT

public:
    static const char* TYPE_QMYSQL;
    static const char* TYPE_QSQLITE;
    static const char* TYPE_QPSQL;
    static const char* TYPE_QOCI;
    static const char* TYPE_QODBC;
    static const char* TYPE_QDB2;
    static const char* TYPE_QTDS;
    static const char* TYPE_QIBASE;

signals:
    void queryOnceWithArgsMap(const QString &sql, const QMap<QString, QVariant> *args, SqlResultPrivate &result, bool &isOk);
    void queryOnceWithArgsList(const QString &sql, const QVector<QVariant> *args, SqlResultPrivate &result, bool &isOk);

public:
    SqlConnection(const QString &type, const QString &databaseName = "",
        const QString &userName = "root", const QString &passowrd = "",
        const QString &host = "127.0.0.1",  int port = 3306);
    ~SqlConnection();

    SqlResult query(const QString &sql, bool *isOk);
    SqlResult query(const QString &sql, const QMap<QString, QVariant> *args, bool *isOk);
    SqlResult query(const QString &sql, const QVector<QVariant> *args, bool *isOk);

    void setAutoOpenInterval(unsigned ms);

private:
    std::shared_ptr<SqlConnectionHandler> m_handler;
    std::thread m_thread;
    std::shared_ptr<QEventLoop> m_eventLoop;
};

class SqlConnectionPool
{
public:
    SqlConnectionPool(unsigned int numConnection, const QString &type, const QString &databaseName = "",
        const QString &userName = "root", const QString &passowrd = "",
        const QString &host = "127.0.0.1",  int port = 3306, unsigned autoOpenInterval = 10000);
    ~SqlConnectionPool();

    const QString& type();
    const QString& databaseName();
    const QString& userName();
    const QString& passowrd();
    const QString& host();
    int port();

    SqlResult query(const QString &sql, bool *isOk = NULL);
    SqlResult query(const QString &sql, const QMap<QString, QVariant> *args, bool *isOk = NULL);
    SqlResult query(const QString &sql, const QVector<QVariant> *args, bool *isOk = NULL);

private:
    QString m_type;
    QString m_databaseName;
    QString m_userName;
    QString m_passowrd;
    QString m_host;
    int m_port;

    std::atomic<unsigned> m_counter;
    QVector<std::shared_ptr<SqlConnection>> m_connections;
};

#endif // SQLCONNECTIONPOOL_H
