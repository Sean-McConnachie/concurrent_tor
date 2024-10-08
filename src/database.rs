use crate::Result;
use crate::{config::DatabaseConfig, execution::scheduler::PlatformT};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::{ColumnIndex, Decode, FromRow, Row, Type};

#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
pub enum JobStatusDb {
    Completed,
    Failed,
    Active,
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
    pub request: String,
}

impl<'a, R: Row, P> FromRow<'a, R> for JobCache<P>
where
    P: PlatformT,
    &'a str: ColumnIndex<R>,
    i32: Decode<'a, R::Database>,
    i32: Type<R::Database>,
    JobStatusDb: Decode<'a, R::Database>,
    JobStatusDb: Type<R::Database>,
    String: Decode<'a, R::Database>,
    String: Type<R::Database>,
{
    fn from_row(row: &'a R) -> std::result::Result<Self, sqlx::Error> {
        let platform_int: i32 = row.try_get("platform")?;
        Ok(Self {
            id: row.try_get("id")?,
            status: row.try_get("status")?,
            platform: P::from_repr(platform_int as usize),
            hash: row.try_get("hash")?,
            num_attempts: row.try_get("num_attempts")?,
            request: row.try_get("request")?,
        })
    }
}

impl<P> JobCache<P>
where
    P: PlatformT,
{
    pub async fn fetch_all_active(pool: &sqlx::PgPool) -> Result<Vec<Self>> {
        let jobs = sqlx::query_as::<_, JobCache<P>>("SELECT * FROM job_cache WHERE status = $1")
            .bind(JobStatusDb::Active)
            .fetch_all(pool)
            .await?;
        Ok(jobs)
    }

    pub async fn insert_new(
        pool: &sqlx::PgPool,
        num_attempts: i32,
        hash: &str,
        platform: P,
        request_json: &str,
    ) -> Result<()> {
        assert_eq!(num_attempts, 0);
        let platform_int: i32 = platform.into();
        sqlx::query(
            "INSERT INTO job_cache (status, platform, hash, num_attempts, request) VALUES ($1, $2, $3, $4, $5)"
        )
            .bind(JobStatusDb::Active)
            .bind(platform_int)
            .bind(hash)
            .bind(num_attempts)
            .bind(request_json)
            .execute(pool)
            .await?;
        Ok(())
    }

    pub async fn update_status_by_hash(
        pool: &sqlx::PgPool,
        hash: &str,
        status: JobStatusDb,
        num_attempts: i32,
    ) -> Result<()> {
        sqlx::query("UPDATE job_cache SET status = $1, num_attempts = $2 WHERE hash = $3")
            .bind(status)
            .bind(num_attempts)
            .bind(hash)
            .execute(pool)
            .await?;
        Ok(())
    }
}

pub async fn connect_pool(config: DatabaseConfig) -> Result<sqlx::PgPool> {
    let pool = PgPoolOptions::new()
        .max_connections(config.maximum_connections)
        .connect(&config.url)
        .await?;
    Ok(pool)
}
