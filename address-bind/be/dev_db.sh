#!/bin/bash
# --db_url postgres://postgres:123456@localhost:5432/postgres
 docker run -d \
        --name web5-address-bind-postgres \
        -p 8080:8080 \
        -p 5432:5432 \
        -e POSTGRES_USER=postgres \
        -e POSTGRES_PASSWORD=123456 \
        -e PGDATA=/var/lib/postgresql/data/pgdata \
        -v ./data:/var/lib/postgresql/data \
        postgres:16.10
