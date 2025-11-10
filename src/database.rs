use async_trait::async_trait;
use chrono::{NaiveDateTime, TimeZone, Utc};
use hbb_common::{log, ResultType};
use serde_derive::Serialize;
use sqlx::Row;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteRow},
    ConnectOptions, Connection, Error as SqlxError, SqliteConnection,
};
use std::{ops::DerefMut, str::FromStr};
//use sqlx::postgres::PgPoolOptions;
//use sqlx::mysql::MySqlPoolOptions;

type Pool = deadpool::managed::Pool<DbPool>;

pub fn resolve_db_url() -> String {
    std::env::var("DB_URL").unwrap_or_else(|_| {
        let mut db = "db_v2.sqlite3".to_owned();
        #[cfg(all(windows, not(debug_assertions)))]
        {
            if let Some(path) = hbb_common::config::Config::icon_path().parent() {
                db = format!("{}\\{}", path.to_str().unwrap_or("."), db);
            }
        }
        #[cfg(not(windows))]
        {
            db = format!("./{db}");
        }
        db
    })
}

pub struct DbPool {
    url: String,
}

#[async_trait]
impl deadpool::managed::Manager for DbPool {
    type Type = SqliteConnection;
    type Error = SqlxError;
    async fn create(&self) -> Result<SqliteConnection, SqlxError> {
        let mut opt = SqliteConnectOptions::from_str(&self.url).unwrap();
        opt.log_statements(log::LevelFilter::Debug);
        SqliteConnection::connect_with(&opt).await
    }
    async fn recycle(
        &self,
        obj: &mut SqliteConnection,
    ) -> deadpool::managed::RecycleResult<SqlxError> {
        Ok(obj.ping().await?)
    }
}

#[derive(Clone)]
pub struct Database {
    pool: Pool,
}

fn as_base64<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let encoded = base64::encode(bytes);
    serializer.serialize_str(&encoded)
}

fn as_base64_option<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match bytes {
        Some(b) => serializer.serialize_str(&base64::encode(b)),
        None => serializer.serialize_none(),
    }
}

#[derive(Default, Clone, Debug, Serialize)]
pub struct Peer {
    #[serde(serialize_with = "as_base64")]
    pub guid: Vec<u8>,
    pub id: String,
    #[serde(serialize_with = "as_base64")]
    pub uuid: Vec<u8>,

    #[serde(serialize_with = "as_base64")]
    pub pk: Vec<u8>,
    pub created_at: i64,
    pub last_heartbeat: i64,

    #[serde(serialize_with = "as_base64_option")]
    pub user: Option<Vec<u8>>,
    pub info: String,
    pub note: Option<String>,
    pub status: Option<i64>,
}

impl Database {
    pub async fn new(url: &str) -> ResultType<Database> {
        if !std::path::Path::new(url).exists() {
            std::fs::File::create(url).ok();
        }
        let n: usize = std::env::var("MAX_DATABASE_CONNECTIONS")
            .unwrap_or_else(|_| "1".to_owned())
            .parse()
            .unwrap_or(1);
        log::debug!("MAX_DATABASE_CONNECTIONS={}", n);
        let pool = Pool::new(
            DbPool {
                url: url.to_owned(),
            },
            n,
        );
        let _ = pool.get().await?; // test
        let db = Database { pool };
        db.create_tables().await?;
        Ok(db)
    }

    async fn create_tables(&self) -> ResultType<()> {
        sqlx::query(
            "
            create table if not exists peer (
                guid blob primary key not null,
                id varchar(100) not null,
                uuid blob not null,
                pk blob not null,
                created_at datetime not null default(current_timestamp),
                last_heartbeat datetime not null default(current_timestamp),
                user blob,
                status tinyint,
                note varchar(300),
                info text not null
            ) without rowid;
            create unique index if not exists index_peer_id on peer (id);
            create index if not exists index_peer_user on peer (user);
            create index if not exists index_peer_created_at on peer (created_at);
            create index if not exists index_peer_status on peer (status);
        ",
        )
        .execute(self.pool.get().await?.deref_mut())
        .await?;
        Ok(())
    }

    fn map_row(row: SqliteRow) -> Peer {
        let create_at_value: String = row.get("created_at");
        let naive_create_at = NaiveDateTime::parse_from_str(&create_at_value, "%Y-%m-%d %H:%M:%S");
        let created_at = if naive_create_at.is_ok() {
            let dt = Utc.from_utc_datetime(&naive_create_at.unwrap());
            dt.timestamp() * 1000
        } else {
            0
        };

        let last_heartbeat_value: String = row.get("last_heartbeat");
        let naive_last_heartbeat =
            NaiveDateTime::parse_from_str(&last_heartbeat_value, "%Y-%m-%d %H:%M:%S");
        let last_heartbeat = if naive_last_heartbeat.is_ok() {
            let dt = Utc.from_utc_datetime(&naive_last_heartbeat.unwrap());
            dt.timestamp() * 1000
        } else {
            0
        };

        Peer {
            guid: row.get("guid"),
            id: row.get("id"),
            uuid: row.get("uuid"),
            pk: row.get("pk"),
            user: row.get("user"),
            created_at,
            last_heartbeat,
            status: row.get("status"),
            info: row.get("info"),
            note: row.get("note"),
        }
    }

    pub async fn get_peer(&self, id: &str) -> ResultType<Option<Peer>> {
        let row = sqlx::query(
            "select guid, id, uuid, pk, user, status, info, note, created_at, last_heartbeat from peer where id = ?",
        )
        .bind(id)
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?;

        Ok(row.map(Self::map_row))
    }

    pub async fn get_peer_by_guid(&self, guid: &[u8]) -> ResultType<Option<Peer>> {
        let row = sqlx::query(
            "select guid, id, uuid, pk, user, status, info, note, created_at, last_heartbeat from peer where guid = ?",
        )
        .bind(guid)
        .fetch_optional(self.pool.get().await?.deref_mut())
        .await?;
        Ok(row.map(Self::map_row))
    }

    pub async fn get_peers(&self) -> ResultType<Vec<Peer>> {
        let rows = sqlx::query(
            "select guid, id, uuid, pk, user, status, info, note, created_at, last_heartbeat from peer",
        )
        .fetch_all(self.pool.get().await?.deref_mut())
        .await?;

        let peers = rows.into_iter().map(Self::map_row).collect();

        Ok(peers)
    }

    pub async fn insert_peer(
        &self,
        id: &str,
        uuid: &[u8],
        pk: &[u8],
        info: &str,
    ) -> ResultType<Vec<u8>> {
        let guid = uuid::Uuid::new_v4().as_bytes().to_vec();
        sqlx::query("insert into peer(guid, id, uuid, pk, info) values(?, ?, ?, ?, ?)")
            .bind(&guid)
            .bind(id)
            .bind(uuid)
            .bind(pk)
            .bind(info)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(guid)
    }

    pub async fn update_pk(
        &self,
        guid: &Vec<u8>,
        id: &str,
        pk: &[u8],
        info: &str,
    ) -> ResultType<()> {
        sqlx::query("update peer set id=?, pk=?, info=? where guid=?")
            .bind(id)
            .bind(pk)
            .bind(info)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_note(&self, guid: &[u8], note: &str) -> ResultType<()> {
        sqlx::query("update peer set note=? where guid=?")
            .bind(note)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_info(&self, guid: &[u8], info: &str) -> ResultType<()> {
        sqlx::query("update peer set info=? where guid=?")
            .bind(info)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_user(&self, guid: &[u8], user: Option<&[u8]>) -> ResultType<()> {
        sqlx::query("update peer set user=? where guid=?")
            .bind(user)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn delete_peer(&self, guid: &[u8]) -> ResultType<()> {
        sqlx::query("delete from peer where guid=?")
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_status_by_id(&self, id: &str, status: i64) -> ResultType<()> {
        sqlx::query("update peer set status=? where id=?")
            .bind(status)
            .bind(id)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_status_and_touch_heartbeat_by_id(
        &self,
        id: &str,
        status: i64,
    ) -> ResultType<u64> {
        let result =
            sqlx::query("update peer set status=?, last_heartbeat=CURRENT_TIMESTAMP where id=?")
                .bind(status)
                .bind(id)
                .execute(self.pool.get().await?.deref_mut())
                .await?;
        Ok(result.rows_affected())
    }

    pub async fn update_status_and_touch_heartbeat_by_guid(
        &self,
        guid: &[u8],
        status: i64,
    ) -> ResultType<()> {
        sqlx::query("update peer set status=?, last_heartbeat=CURRENT_TIMESTAMP where guid=?")
            .bind(status)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn update_status_by_guid(&self, guid: &[u8], status: i64) -> ResultType<()> {
        sqlx::query("update peer set status=? where guid=?")
            .bind(status)
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn touch_heartbeat_by_guid(&self, guid: &[u8]) -> ResultType<()> {
        sqlx::query("update peer set last_heartbeat=CURRENT_TIMESTAMP where guid=?")
            .bind(guid)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }

    pub async fn touch_heartbeat_by_id(&self, id: &str) -> ResultType<()> {
        sqlx::query("update peer set last_heartbeat=CURRENT_TIMESTAMP where id=?")
            .bind(id)
            .execute(self.pool.get().await?.deref_mut())
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use hbb_common::tokio;
    #[test]
    fn test_insert() {
        insert();
    }

    #[tokio::main(flavor = "multi_thread")]
    async fn insert() {
        let db = super::Database::new("test.sqlite3").await.unwrap();
        let mut jobs = vec![];
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                let empty_vec = Vec::new();
                cloned
                    .insert_peer(&id, &empty_vec, &empty_vec, "")
                    .await
                    .unwrap();
            });
            jobs.push(a);
        }
        for i in 0..10000 {
            let cloned = db.clone();
            let id = i.to_string();
            let a = tokio::spawn(async move {
                cloned.get_peer(&id).await.unwrap();
            });
            jobs.push(a);
        }
        hbb_common::futures::future::join_all(jobs).await;
    }
}
