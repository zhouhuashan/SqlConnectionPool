QT += core sql
QT -= gui

CONFIG += c++11

TARGET = SqlConnectionPool
CONFIG += console
CONFIG -= app_bundle

TEMPLATE = app

SOURCES += main.cpp \
    SqlConnectionPool.cpp

HEADERS += \
    SqlConnectionPool.h
