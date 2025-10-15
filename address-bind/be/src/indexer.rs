use crate::{Indexer, error::AppError, verify::verify_tx};
use ckb_jsonrpc_types::BlockNumber;
use ckb_sdk::{NetworkType, rpc::CkbRpcClient};
use color_eyre::{Result, eyre::eyre};
use common_x::restful::{
    axum::{
        Router,
        extract::{Path, State},
        response::IntoResponse,
        routing::get,
    },
    ok,
};
use sqlx::{Executor, postgres::PgPoolOptions, query, query_as};
use std::time::Duration;
use tokio::time::sleep;
use tower_http::{cors::CorsLayer, timeout::TimeoutLayer};

// define query handler
async fn query_by_from(
    State(state): State<Indexer>,
    Path(from): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    let rows: Vec<(String, i64, i32)> = query_as(
        "SELECT to_addr, height, tx_index
         FROM bind_info
         WHERE from_addr = $1
         ORDER BY height DESC, tx_index DESC",
    )
    .bind(&from)
    .fetch_all(&state.db)
    .await
    .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let result: Vec<_> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "to": row.0,
                "height": row.1,
                "tx_index": row.2
            })
        })
        .collect();

    Ok(ok(result))
}

async fn query_by_to(
    State(state): State<Indexer>,
    Path(to): Path<String>,
) -> Result<impl IntoResponse, AppError> {
    // Select for each from_addr the row with max height, and within that height the max tx_index
    // Use DISTINCT ON to ensure we only return the latest record per from_addr
    let rows: Vec<(String, i64, i32)> = query_as(
        "SELECT DISTINCT ON (from_addr) from_addr, height, tx_index
         FROM bind_info
         WHERE to_addr = $1
         ORDER BY from_addr, height DESC, tx_index DESC",
    )
    .bind(&to)
    .fetch_all(&state.db)
    .await
    .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let result: Vec<_> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "from": row.0,
                "height": row.1,
                "tx_index": row.2
            })
        })
        .collect();

    Ok(ok(result))
}

// like query_by_to, but excludes rows with height greater than specified
async fn query_by_to_at_height(
    State(state): State<Indexer>,
    Path((to, height)): Path<(String, i64)>,
) -> Result<impl IntoResponse, AppError> {
    let rows: Vec<(String, i64, i32)> = query_as(
        "SELECT DISTINCT ON (from_addr) from_addr, height, tx_index
         FROM bind_info
         WHERE to_addr = $1 AND height <= $2
         ORDER BY from_addr, height DESC, tx_index DESC",
    )
    .bind(&to)
    .bind(height)
    .fetch_all(&state.db)
    .await
    .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let result: Vec<_> = rows
        .iter()
        .map(|row| {
            serde_json::json!({
                "from": row.0,
                "height": row.1,
                "tx_index": row.2
            })
        })
        .collect();

    Ok(ok(result))
}

pub async fn server(
    ckb_client: &CkbRpcClient,
    network_type: NetworkType,
    db_url: &str,
    start_height: u64,
    listen_port: u16,
    recovery_url: &str,
) -> Result<()> {
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_url)
        .await?;

    // create table
    db.execute("CREATE TABLE IF NOT EXISTS sync_status (height BIGINT PRIMARY KEY)")
        .await?;
    db.execute(
        "CREATE TABLE IF NOT EXISTS bind_info (from_addr TEXT, to_addr TEXT, timestamp BIGINT, height BIGINT, tx_index INTEGER, UNIQUE(from_addr, to_addr, timestamp))",
    ).await?;

    // get last sync height
    let mut current_height: u64 =
        query_as("SELECT height FROM sync_status ORDER BY height DESC LIMIT 1")
            .fetch_one(&db)
            .await
            .map(|r: (i32,)| r.0 as u64)
            .unwrap_or(start_height);

    let indexer = Indexer { db: db.clone() };

    tokio::spawn(async move {
        // start http server on listen_port
        let app = Router::new()
            .route("/by_from/{from}", get(query_by_from))
            .route("/by_to/{to}", get(query_by_to))
            .route("/by_to_at_height/{to}/{height}", get(query_by_to_at_height))
            .layer((TimeoutLayer::new(Duration::from_secs(10)),))
            .layer(CorsLayer::permissive())
            .with_state(indexer);

        common_x::restful::http_serve(listen_port, app)
            .await
            .map_err(|e| eyre!("{e}"))
    });

    loop {
        // get latest block height
        if let Ok(tip_block) = ckb_client.get_tip_block_number() {
            // if already synced to latest height, wait for new block
            let tip_block = tip_block.value();
            if current_height >= tip_block {
                sleep(Duration::from_secs(1)).await;
                continue;
            } else if current_height.is_multiple_of(10) {
                info!(
                    "tip_block: {tip_block}, current_height: {current_height}, waiting block: {}",
                    tip_block - current_height
                );
            }
        } else {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        // get block by number
        let ret = ckb_client.get_block_by_number(BlockNumber::from(current_height));

        if let Ok(Some(block)) = ret {
            let block_timestamp = u64::from(block.header.inner.timestamp);

            // proc transactions in block
            for (index, tx) in block.transactions.into_iter().enumerate() {
                // ignore cellbase transaction
                if index == 0 {
                    continue;
                }

                // verify transaction
                match verify_tx(ckb_client, network_type, &tx.inner, recovery_url).await {
                    Ok((from, to, timestamp)) => {
                        info!("from: {from}, to: {to}, timestamp: {timestamp}");

                        // check timestamp is around current block timestamp, within 20min
                        if timestamp < block_timestamp - 20 * 60 * 1000
                            || timestamp > block_timestamp + 20 * 60 * 1000
                        {
                            error!(
                                "timestamp {timestamp} is out of range, block_timestamp: {block_timestamp}"
                            );
                            continue;
                        }

                        // insert bind info to db
                        if let Err(e) = db
                            .execute(query(
                                "INSERT INTO bind_info (from_addr, to_addr, timestamp, height, tx_index)
                                 VALUES ($1, $2, $3, $4, $5)"
                            )
                            .bind(&from)
                            .bind(&to)
                            .bind(timestamp as i64)
                            .bind(current_height as i64)
                            .bind(index as i32))
                        .await {
                            error!("Failed to insert bind info: {e}");
                        }
                    }
                    Err(e) => {
                        if e.contains("get_tx failed")
                            || e.contains("sig_bytes")
                            || e.contains("timestamp is out of range")
                        {
                            error!("verify_tx {} is failed, err: {e}", tx.hash);
                        }
                    }
                }
            }

            // update sync height
            // not too frequently
            if current_height.is_multiple_of(100)
                && let Err(e) = db
                    .execute(query(
                        "INSERT INTO sync_status (height) VALUES ($1) ON CONFLICT (height) DO NOTHING;"
                    )
                    .bind(current_height as i64))
                    .await
            {
                error!("Failed to update sync status: {e}");
            }

            current_height += 1;
        }
    }
}

#[tokio::test]
async fn test_one() -> Result<()> {
    common_x::log::init_log_filter("info");
    let ckb_client = CkbRpcClient::new("https://testnet.ckb.dev/");
    let ret = ckb_client.get_block_by_number(BlockNumber::from(18977278));

    if let Ok(Some(block)) = ret {
        let block_timestamp = u64::from(block.header.inner.timestamp);
        info!("block_timestamp: {block_timestamp}");
        // proc transactions in block
        for (index, tx) in block.transactions.into_iter().enumerate() {
            // ignore cellbase transaction
            if index == 0 {
                continue;
            }

            // verify transaction
            match verify_tx(&ckb_client, NetworkType::Testnet, &tx.inner).await {
                Ok((from, to, timestamp)) => {
                    info!("from: {from}, to: {to}, timestamp: {timestamp}");
                    // check timestamp is around current block timestamp, within 20min
                    if timestamp < block_timestamp - 20 * 60 * 1000
                        || timestamp > block_timestamp + 20 * 60 * 1000
                    {
                        error!(
                            "timestamp {timestamp} is out of range, block_timestamp: {block_timestamp}"
                        );
                        continue;
                    }
                }
                Err(e) => {
                    if e.contains("get_tx failed") {
                        error!("verify_tx {} is failed, err: {e}", tx.hash);
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::{Pool, Postgres};

    async fn get_test_db() -> Option<Pool<Postgres>> {
        let db_url = std::env::var("TEST_DB_URL")
            .or_else(|_| std::env::var("DB_URL"))
            .ok()?;
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await
            .ok()
    }

    fn test_schema(suffix: &str) -> String {
        format!("abtest_{}_{}", std::process::id(), suffix)
    }

    async fn setup_schema(db: &Pool<Postgres>, s: &str) -> Result<()> {
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {s}"))
            .execute(db)
            .await?;
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {s}.sync_status (height BIGINT PRIMARY KEY)"
        ))
        .execute(db)
        .await?;
        sqlx::query(&format!("CREATE TABLE IF NOT EXISTS {s}.bind_info (from_addr TEXT, to_addr TEXT, timestamp BIGINT, height BIGINT, tx_index INTEGER, UNIQUE(from_addr, to_addr, timestamp))")).execute(db).await?;
        sqlx::query(&format!("TRUNCATE TABLE {s}.bind_info RESTART IDENTITY"))
            .execute(db)
            .await?;
        sqlx::query(&format!("TRUNCATE TABLE {s}.sync_status RESTART IDENTITY"))
            .execute(db)
            .await?;
        Ok(())
    }

    async fn seed_data(db: &Pool<Postgres>, s: &str) -> Result<()> {
        // F1 -> T1 with multiple heights and tx_index
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F1").bind("T1").bind(0_i64).bind(101_i64).bind(3_i32)).await?;
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F1").bind("T1").bind(1_i64).bind(103_i64).bind(1_i32)).await?;
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F1").bind("T1").bind(2_i64).bind(103_i64).bind(5_i32)).await?; // latest by height then tx_index
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F1").bind("T1").bind(3_i64).bind(102_i64).bind(9_i32)).await?;

        // F2 -> T1
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F2").bind("T1").bind(4_i64).bind(88_i64).bind(7_i32)).await?;
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F2").bind("T1").bind(5_i64).bind(100_i64).bind(2_i32)).await?;
        db.execute(query(&format!("INSERT INTO {s}.bind_info (from_addr, to_addr, timestamp, height, tx_index) VALUES ($1, $2, $3, $4, $5)"))
            .bind("F2").bind("T1").bind(6_i64).bind(100_i64).bind(6_i32)).await?; // latest by height then tx_index

        Ok(())
    }

    #[tokio::test]
    async fn test_query_by_to_distinct_on() -> Result<()> {
        let Some(db) = get_test_db().await else {
            eprintln!("Skipped test_query_by_to_distinct_on: TEST_DB_URL/DB_URL not set");
            return Ok(());
        };
        let s = test_schema("to");
        setup_schema(&db, &s).await?;
        seed_data(&db, &s).await?;

        let sql = format!(
            "SELECT DISTINCT ON (from_addr) from_addr, height, tx_index
                           FROM {s}.bind_info
                           WHERE to_addr = $1
                           ORDER BY from_addr, height DESC, tx_index DESC"
        );
        let rows: Vec<(String, i64, i32)> = query_as(&sql).bind("T1").fetch_all(&db).await?;

        // Expect F1->height 103, tx_index=5, F2->height 100, tx_index=6
        let mut map = std::collections::HashMap::new();
        for (from, height, tx_index) in rows {
            map.insert(from, (height, tx_index));
        }
        assert_eq!(map.get("F1"), Some(&(103_i64, 5_i32)));
        assert_eq!(map.get("F2"), Some(&(100_i64, 6_i32)));
        Ok(())
    }

    #[tokio::test]
    async fn test_query_by_from_ordering() -> Result<()> {
        let Some(db) = get_test_db().await else {
            eprintln!("Skipped test_query_by_from_ordering: TEST_DB_URL/DB_URL not set");
            return Ok(());
        };
        let s = test_schema("from");
        setup_schema(&db, &s).await?;
        seed_data(&db, &s).await?;

        let sql = format!(
            "SELECT to_addr, height, tx_index
                           FROM {s}.bind_info
                           WHERE from_addr = $1
                           ORDER BY height DESC, tx_index DESC"
        );
        let rows: Vec<(String, i64, i32)> = query_as(&sql).bind("F1").fetch_all(&db).await?;

        // First row should be height=103, tx_index=5
        assert!(!rows.is_empty());
        let (to, height, tx_index) = &rows[0];
        assert_eq!(to, "T1");
        assert_eq!(*height, 103_i64);
        assert_eq!(*tx_index, 5_i32);
        Ok(())
    }

    #[tokio::test]
    async fn test_query_by_to_at_height_cutoff() -> Result<()> {
        let Some(db) = get_test_db().await else {
            eprintln!("Skipped test_query_by_to_at_height_cutoff: TEST_DB_URL/DB_URL not set");
            return Ok(());
        };
        let s = test_schema("toheight");
        setup_schema(&db, &s).await?;
        seed_data(&db, &s).await?;

        // Same data as before; cut off at height <= 100
        let sql = format!(
            "SELECT DISTINCT ON (from_addr) from_addr, height, tx_index
                         FROM {s}.bind_info
                         WHERE to_addr = $1 AND height <= $2
                         ORDER BY from_addr, height DESC, tx_index DESC"
        );
        let rows: Vec<(String, i64, i32)> = query_as(&sql)
            .bind("T1")
            .bind(100_i64)
            .fetch_all(&db)
            .await?;

        // Expect F1 excluded (latest at 103), F2 included with height 100, tx_index=6
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "F2");
        assert_eq!(rows[0].1, 100_i64);
        assert_eq!(rows[0].2, 6_i32);
        Ok(())
    }
}
