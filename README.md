# psqlconn

![GitHub contributors](https://img.shields.io/github/contributors/sivaosorg/gocell)
![GitHub followers](https://img.shields.io/github/followers/sivaosorg)
![GitHub User's stars](https://img.shields.io/github/stars/pnguyen215)

A Golang PostgreSQL connector library with comprehensive functionality, including database creation, batch execution, and transaction management.

## Table of Contents

- [psqlconn](#psqlconn)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Modules](#modules)
    - [Running Tests](#running-tests)
    - [Tidying up Modules](#tidying-up-modules)
    - [Upgrading Dependencies](#upgrading-dependencies)
    - [Cleaning Dependency Cache](#cleaning-dependency-cache)

## Introduction

Welcome to the Postgres Connector for Go repository! This library provides a robust set of features for interacting with PostgreSQL databases in your Go applications. It includes the ability to create new databases, execute batch operations, and manage transactions efficiently.

## Prerequisites

Golang version v1.20

## Installation

- Latest version

```bash
go get -u github.com/sivaosorg/psqlconn@latest
```

- Use a specific version (tag)

```bash
go get github.com/sivaosorg/psqlconn@v0.0.1
```

## Modules

Explain how users can interact with the various modules.

### Running Tests

To run tests for all modules, use the following command:

```bash
make test
```

### Tidying up Modules

To tidy up the project's Go modules, use the following command:

```bash
make tidy
```

### Upgrading Dependencies

To upgrade project dependencies, use the following command:

```bash
make deps-upgrade
```

### Cleaning Dependency Cache

To clean the Go module cache, use the following command:

```bash
make deps-clean-cache
```
