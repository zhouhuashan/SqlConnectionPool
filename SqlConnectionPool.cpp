#include "SqlConnectionPool.h"
#include <quuid.h>
#include <qthread.h>

std::mutex SqlConnectionHandler::s_mutex;

const char* SqlConnection::TYPE_QMYSQL = "QMYSQL";
const char* SqlConnection::TYPE_QSQLITE = "QSQLITE";
const char* SqlConnection::TYPE_QPSQL = "QPSQL";
const char* SqlConnection::TYPE_QOCI = "QOCI";
const char* SqlConnection::TYPE_QODBC = "QODBC";
const char* SqlConnection::TYPE_QDB2 = "QDB2";
const char* SqlConnection::TYPE_QTDS = "QTDS";
const char* SqlConnection::TYPE_QIBASE = "QIBASE";

SqlConnectionHandler::SqlConnectionHandler(const QString &type, const QString &databaseName,
    const QString &userName, const QString &password, const QString &host, int port)
    :   m_handler(NULL),
        m_autoOpenInterval(10000),
        m_type(type),
        m_databaseName(databaseName),
        m_userName(userName),
        m_password(password),
        m_host(host),
        m_port(port),
        m_testSql("SELECT 1;")
{
    if (type == SqlConnection::TYPE_QOCI)
        m_testSql = "SELECT 1 FROM dual";
}

SqlConnectionHandler::~SqlConnectionHandler()
{
    releaseHandler();
}

bool SqlConnectionHandler::open()
{
    std::lock_guard<std::mutex> guard(s_mutex);
    return m_handler->open();
}

void SqlConnectionHandler::setAutoOpenInterval(unsigned ms)
{
    m_autoOpenInterval = ms;
}

bool SqlConnectionHandler::checkConnection(QSqlError &err)
{
    bool ret = true;

    if (!m_handler)
        initHandler();

    do
    {
        if (!m_handler->isOpen())
        {
            err = m_handler->lastError();
            QDateTime now = QDateTime::currentDateTime();

            if (m_lastOpenTime.isValid() && m_lastOpenTime.addMSecs(m_autoOpenInterval) > now)
            {
                ret = false;
                break;
            }

            m_lastOpenTime = now;
            if(!open())
            {
                err = m_handler->lastError();
                releaseHandler();
                ret = false;
                break ;
            }
        }
    } while(false);

    return ret;
}

void SqlConnectionHandler::doQeuryWithArgsMap(const QString &sql, const QMap<QString, QVariant> *args, SqlResultPrivate &result, bool &isOk)
{
    doQuery<QMap<QString, QVariant>>(sql, args, result, isOk);
}

void SqlConnectionHandler::doQeuryWithArgsList(const QString &sql, const QVector<QVariant> *args, SqlResultPrivate &result, bool &isOk)
{
    doQuery<QVector<QVariant>>(sql, args, result, isOk);
}

bool SqlConnectionHandler::doQeury(QSqlQuery &query, SqlResultPrivate &result, bool &isOk)
{
    bool ret = true;
    isOk = true;

    if(query.exec())
    {
        result.error = query.lastError();
        while (query.next())
        {
            result.records.push_back(query.record());
        }
    }
    else
    {
        isOk = false;
        result.error = query.lastError();
        if (!query.exec(m_testSql))
        {
            result.error = query.lastError();
            ret = false;
        }
    }

    result.lastInsertId = query.lastInsertId();
    result.numRowsAffected = query.numRowsAffected();

    return ret;
}

void SqlConnectionHandler::initHandler()
{
    {
        std::lock_guard<std::mutex> guard(s_mutex);
        m_handler = new QSqlDatabase(QSqlDatabase::addDatabase(m_type, QUuid::createUuid().toString()));
    }

    m_handler->setDatabaseName(m_databaseName);
    m_handler->setUserName(m_userName);
    m_handler->setPassword(m_password);
    m_handler->setHostName(m_host);
    m_handler->setPort(m_port);
}

void SqlConnectionHandler::releaseHandler()
{
    std::lock_guard<std::mutex> guard(s_mutex);
    if (m_handler)
    {
        QString connectionName = m_handler->connectionName();

        delete m_handler;
        m_handler = NULL;
        QSqlDatabase::removeDatabase(connectionName);
    }
}

void SqlConnectionHandler::prepareArgs(QSqlQuery &query, const QMap<QString, QVariant> *args)
{
    if (args)
    {
        for (QMap<QString, QVariant>::const_iterator it = args->begin(); it != args->end(); ++it)
            query.bindValue(it.key(), it.value());
    }
}

void SqlConnectionHandler::prepareArgs(QSqlQuery &query, const QVector<QVariant> *args)
{
    if (args)
    {
        int i = 0;
        for (auto arg : *args)
        {
            query.bindValue(i, arg);
            ++i;
        }
    }
}

SqlConnection::SqlConnection(const QString &type, const QString &databaseName, const QString &userName,
    const QString &password, const QString &host, int port)
{
    bool ready = false;
    m_thread = std::thread([this, &type, &databaseName, &userName, &password, &host, &port, &ready]()
    {
        m_handler.reset(new SqlConnectionHandler(type, databaseName, userName, password, host, port));
        QObject::connect(this, SIGNAL(queryOnceWithArgsList(QString,const QVector<QVariant>*,SqlResultPrivate&,bool&)),
            m_handler.get(), SLOT(doQeuryWithArgsList(QString,const QVector<QVariant>*,SqlResultPrivate&,bool&)),
            Qt::BlockingQueuedConnection);

        QObject::connect(this, SIGNAL(queryOnceWithArgsMap(QString,const QMap<QString,QVariant>*,SqlResultPrivate&,bool&)),
            m_handler.get(), SLOT(doQeuryWithArgsMap(QString,const QMap<QString,QVariant>*,SqlResultPrivate&,bool&)),
            Qt::BlockingQueuedConnection);

        m_eventLoop.reset(new QEventLoop);
        ready = true;
        m_eventLoop->exec();
    });
    while (!ready)
        QThread::msleep(1);
}

SqlConnection::~SqlConnection()
{
    if (m_eventLoop.get())
        m_eventLoop->exit();

    if (m_thread.joinable())
        m_thread.join();
}

SqlResult SqlConnection::query(const QString &sql, bool *isOk)
{
    std::shared_ptr<SqlResultPrivate> result(new SqlResultPrivate());
    bool tmpIsOk;

    emit queryOnceWithArgsMap(sql, NULL, *result, tmpIsOk);
    if (isOk)
        *isOk = tmpIsOk;

    return SqlResult(result);
}

SqlResult SqlConnection::query(const QString &sql, const QMap<QString, QVariant> *args, bool *isOk)
{
    std::shared_ptr<SqlResultPrivate> result(new SqlResultPrivate());
    bool tmpIsOk;

    emit queryOnceWithArgsMap(sql, args, *result, tmpIsOk);
    if (isOk)
        *isOk = tmpIsOk;

    return SqlResult(result);
}

SqlResult SqlConnection::query(const QString &sql, const QVector<QVariant> *args, bool *isOk)
{
    std::shared_ptr<SqlResultPrivate> result(new SqlResultPrivate());
    bool tmpIsOk;

    emit queryOnceWithArgsList(sql, args, *result, tmpIsOk);
    if (isOk)
        *isOk = tmpIsOk;

    return SqlResult(result);
}

void SqlConnection::setAutoOpenInterval(unsigned ms)
{
    if (m_handler.get())
        m_handler->setAutoOpenInterval(ms);
}


SqlConnectionPool::SqlConnectionPool(unsigned int numConnection,
    const QString & type, const QString &databaseName, const QString &userName,
    const QString &password, const QString &host, int port,
    unsigned autoOpenInterval)
    :   m_type(type),
        m_databaseName(databaseName),
        m_userName(userName),
        m_password(password),
        m_host(host),
        m_port(port),
        m_counter(0)
{
    Q_ASSERT_X(numConnection, "SqlConnectionPool", "numConnection == 0");
    for (unsigned int i = 0; i < numConnection; ++i)
    {
        SqlConnection *conn = new SqlConnection(type, databaseName, userName, password, host, port);
        conn->setAutoOpenInterval(autoOpenInterval);
        m_connections.push_back(std::shared_ptr<SqlConnection>(conn));
    }
}

SqlConnectionPool::~SqlConnectionPool()
{

}

const QString &SqlConnectionPool::type()
{
    return m_type;
}

const QString &SqlConnectionPool::databaseName()
{
    return m_databaseName;
}

const QString &SqlConnectionPool::userName()
{
    return m_userName;
}

const QString &SqlConnectionPool::password()
{
    return m_password;
}

const QString &SqlConnectionPool::host()
{
    return m_host;
}

int SqlConnectionPool::port()
{
    return m_port;
}

SqlResult SqlConnectionPool::query(const QString &sql, bool *isOk)
{
    unsigned count = ++m_counter;
    return m_connections.at(count % m_connections.size())->query(sql, isOk);
}

SqlResult SqlConnectionPool::query(const QString &sql, const QMap<QString, QVariant> *args, bool *isOk)
{
    unsigned count = ++m_counter;
    return m_connections.at(count % m_connections.size())->query(sql, args, isOk);
}

SqlResult SqlConnectionPool::query(const QString &sql, const QVector<QVariant> *args, bool *isOk)
{
    unsigned count = ++m_counter;
    return m_connections.at(count % m_connections.size())->query(sql, args, isOk);
}

SqlResult::SqlResult(std::shared_ptr<SqlResultPrivate> result)
    :   m_result(result)
{

}

SqlResult::~SqlResult()
{

}

const QSqlError &SqlResult::error()
{
    return m_result->error;
}

const SqlRecords &SqlResult::records()
{
    return m_result->records;
}

const QVariant &SqlResult::lastInsertId()
{
    return m_result->lastInsertId;
}

int SqlResult::numRowsAffected()
{
    return m_result->numRowsAffected;
}
