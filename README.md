#MySQL ESM for GER

[![Build Status](https://travis-ci.org/nikashitsa/ger_mysql_esm.svg?branch=master)](https://travis-ci.org/nikashitsa/ger_mysql_esm)

This is a project that implements an Event Store Manager in MySQL for [GER](https://www.npmjs.com/package/ger).

Work in progress... Need help with some issues.

### Testing

First checkout the repository, then

```
npm install
```

then start the mysql server with [docker-compose](https://docs.docker.com/compose/)

```
docker-compose up -d
```

then execute the test suite with

```
mocha
```
