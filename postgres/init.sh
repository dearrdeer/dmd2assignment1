#!/usr/bin/env bash

psql -U postgres -d dvdrental -f "/var/lib/postgresql/dump/restore.sql"
