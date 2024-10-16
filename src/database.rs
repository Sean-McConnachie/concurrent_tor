use crate::{execution::scheduler::PlatformT, Result};
use serde::{Deserialize, Serialize};
use sqlx::{
    sqlite::SqliteConnectOptions, ColumnIndex, Connection, Decode, FromRow, Row, SqliteConnection,
    Type,
};
use strum::FromRepr;

pub type DB = SqliteConnection;

#[derive(Debug, Serialize, Deserialize, sqlx::Type, FromRepr, Clone, Copy)]
pub enum JobStatusDb {
    Active,
    Completed,
    Failed,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobCache<P>
where
    P: 'static,
{
    pub id: i32,
    pub status: JobStatusDb,
    pub platform: P,
    pub hash: String,
    pub num_attempts: i32,
    pub max_attempts: i32,
    pub request: String,
}

impl<'a, R: Row, P> FromRow<'a, R> for JobCache<P>
where
    P: PlatformT,
    &'a str: ColumnIndex<R>,
    i32: Decode<'a, R::Database>,
    i32: Type<R::Database>,
    String: Decode<'a, R::Database>,
    String: Type<R::Database>,
{
    fn from_row(row: &'a R) -> std::result::Result<Self, sqlx::Error> {
        let platform_int: i32 = row.try_get("platform")?;
        let status_int: i32 = row.try_get("status")?;
        Ok(Self {
            id: row.try_get("id")?,
            status: JobStatusDb::from_repr(status_int as usize).unwrap(),
            platform: P::from_repr(platform_int as usize),
            hash: row.try_get("hash")?,
            num_attempts: row.try_get("num_attempts")?,
            max_attempts: row.try_get("max_attempts")?,
            request: row.try_get("request")?,
        })
    }
}

impl<P> JobCache<P>
where
    P: PlatformT,
{
    pub async fn fetch_all_active(pool: &mut DB) -> Result<Vec<Self>> {
        let jobs = sqlx::query_as::<_, JobCache<P>>("SELECT * FROM job_cache WHERE status = $1")
            .bind(JobStatusDb::Active as i32)
            .fetch_all(pool)
            .await?;
        Ok(jobs)
    }

    pub async fn insert_new(
        pool: &mut DB,
        num_attempts: i32,
        max_attempts: i32,
        hash: u128,
        platform: P,
        request_json: &str,
    ) -> Result<()> {
        assert_eq!(num_attempts, 0);
        let hash = hash.to_string();
        let platform_int = platform.to_repr() as i32;
        sqlx::query(
            "INSERT INTO job_cache (status, platform, hash, num_attempts, max_attempts, request) \
                    VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(JobStatusDb::Active as i32)
        .bind(platform_int)
        .bind(hash)
        .bind(num_attempts)
        .bind(max_attempts)
        .bind(request_json)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn update_status_by_hash(
        pool: &mut DB,
        hash: u128,
        status: JobStatusDb,
        num_attempts: i32,
    ) -> Result<()> {
        let hash = hash.to_string();
        sqlx::query("UPDATE job_cache SET status = $1, num_attempts = $2 WHERE hash = $3")
            .bind(status as i32)
            .bind(num_attempts)
            .bind(hash)
            .execute(pool)
            .await?;
        Ok(())
    }
}

pub async fn connect_and_init_db() -> Result<DB> {
    const DATABASE_FP: &str = "concurrent_tor.sqlite3";
    let options = SqliteConnectOptions::new()
        .filename(DATABASE_FP)
        .create_if_missing(true);
    let mut pool = SqliteConnection::connect_with(&options).await?;
    init_db(&mut pool).await?;
    Ok(pool)
}

async fn init_db(pool: &mut DB) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS job_cache (
            id INTEGER PRIMARY KEY,
            status INTEGER NOT NULL,
            platform INTEGER NOT NULL,
            hash TEXT NOT NULL,
            num_attempts INTEGER NOT NULL,
            max_attempts INTEGER NOT NULL,
            request TEXT NOT NULL,
            UNIQUE (hash)

            ON CONFLICT FAIL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}
