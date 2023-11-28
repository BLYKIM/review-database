//! Database backup utilities.

use crate::{Database, Event, EventDb, RecordType, Store};
use anyhow::Result;
use chrono::{DateTime, TimeZone, Utc};
use flate2::{write::GzEncoder, Compression};
use rocksdb::backup::BackupEngineInfo;
use std::{path::Path, sync::Arc, time::Duration};
use tokio::sync::{Notify, RwLock};
use tracing::{error, info, warn};

#[allow(clippy::module_name_repetitions)]
pub struct BackupInfo {
    pub id: u32,
    pub timestamp: DateTime<Utc>,
    pub size: u64,
}

impl From<BackupEngineInfo> for BackupInfo {
    fn from(backup: BackupEngineInfo) -> Self {
        Self {
            id: backup.backup_id,
            timestamp: Utc.timestamp_nanos(backup.timestamp),
            size: backup.size,
        }
    }
}

/// Schedules periodic database backups.
#[allow(clippy::module_name_repetitions)]
pub async fn schedule_periodic(
    store: Arc<RwLock<Store>>,
    db: Database,
    schedule: (Duration, Duration), // time, interval
    backups_to_keep: u32,
    stop: Arc<Notify>,
    retentions: (Duration, Duration), // db, stat
    export_path: &Path,
) {
    use tokio::time::{sleep, Instant};

    let (init, duration) = schedule;
    let (db_retention, stat_retention) = retentions;
    let db_retention_ts = retention_timestamp(db_retention).unwrap_or(0);
    let stat_retention_time = Utc::now().naive_utc() - stat_retention;
    let sleep = sleep(init);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            () = &mut sleep => {
                sleep.as_mut().reset(Instant::now() + duration);
                let _res = create(&store, false, backups_to_keep);
                let _res = export_events(&store, export_path, db_retention_ts);
                let _res = retain_db(&store, db_retention_ts);
                let _res = db.retain_column_statistics(stat_retention_time);
            }
            () = stop.notified() => {
                info!("creating a database backup before shutdown");
                let _res = create(&store, false, backups_to_keep);
                stop.notify_one();
                return;
            }
        }
    }
}

/// Creates a new database backup, keeping the specified number of backups.
///
/// # Errors
///
/// Returns an error if backup fails.
pub async fn create(store: &Arc<RwLock<Store>>, flush: bool, backups_to_keep: u32) -> Result<()> {
    // TODO: This function should be expanded to support PostgreSQL backups as well.
    info!("backing up database...");
    let res = {
        let mut store = store.write().await;
        store.backup(flush, backups_to_keep)
    };
    match res {
        Ok(()) => {
            info!("backing up database completed");
            Ok(())
        }
        Err(e) => {
            warn!("database backup failed: {:?}", e);
            Err(e)
        }
    }
}

/// Lists the backup information of the database.
///
/// # Errors
///
/// Returns an error if backup list fails to create
pub async fn list(store: &Arc<RwLock<Store>>) -> Result<Vec<BackupInfo>> {
    // TODO: This function should be expanded to support PostgreSQL backups as well.
    let res = {
        let store = store.read().await;
        store.get_backup_info()
    };
    match res {
        Ok(backup_list) => {
            info!("generate database backup list");
            Ok(backup_list
                .into_iter()
                .map(std::convert::Into::into)
                .collect())
        }
        Err(e) => {
            warn!("failed to generate backup list: {:?}", e);
            Err(e)
        }
    }
}

/// Restores the database from a backup with the specified ID.
///
/// # Errors
///
/// Returns an error if the restore operation fails.
pub async fn restore(store: &Arc<RwLock<Store>>, backup_id: Option<u32>) -> Result<()> {
    // TODO: This function should be expanded to support PostgreSQL backups as well.
    info!("restoring database from {:?}", backup_id);
    let res = {
        let mut store = store.write().await;
        match &backup_id {
            Some(id) => store.restore_from_backup(*id),
            None => store.restore_from_latest_backup(),
        }
    };

    match res {
        Ok(()) => {
            info!("database restored from backup {:?}", backup_id);
            Ok(())
        }
        Err(e) => {
            warn!(
                "failed to restore database from backup {:?}: {:?}",
                backup_id, e
            );
            Err(e)
        }
    }
}

/// Restores the database from a backup with the specified ID.
///
/// # Errors
///
/// Returns an error if the restore operation fails.
pub async fn recover(store: &Arc<RwLock<Store>>) -> Result<()> {
    // TODO: This function should be expanded to support PostgreSQL backups as well.
    info!("recovering database from latest valid backup");
    let res = {
        let mut store = store.write().await;
        store.recover()
    };

    match res {
        Ok(()) => {
            info!("database recovered from backup");
            Ok(())
        }
        Err(e) => {
            warn!("failed to recover database from backup: {e:?}");
            Err(e)
        }
    }
}

/// Exports the events older than retention time.
///
/// # Errors
///
/// Returns an error if export fails
pub async fn export_events(
    store: &Arc<RwLock<Store>>,
    export_path: &Path,
    retention_timestamp: i64,
) -> Result<()> {
    let store = store.read().await;
    let map = store.events();

    match export_events_to(&map, export_path, retention_timestamp) {
        Ok((success, failed)) => info!("{success} events are exported. {failed} failed."),
        Err(e) => error!("failed to export detected events. {e:?}"),
    }

    Ok(())
}

/// Retain events with retention time.
///
/// # Errors
///
/// Returns an error if database fails to retain
pub async fn retain_db(store: &Arc<RwLock<Store>>, retention_timestamp: i64) -> Result<()> {
    info!("Retaining events database...");
    let res = {
        let store = store.write().await;
        let old_keys = store.events().get_old(retention_timestamp);
        store.events().delete_old(&old_keys)
    };

    match res {
        Ok(()) => {
            info!("Retaining events database completed");
            Ok(())
        }
        Err(e) => {
            warn!("Retaining events database failed: {:?}", e);
            Err(e)
        }
    }
}

fn retention_timestamp(db_retention: Duration) -> Result<i64> {
    let retention_duration = i64::try_from(db_retention.as_nanos())?;
    Ok(Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or(retention_duration)
        - retention_duration)
}

fn export_events_to(
    db: &EventDb,
    export_path: &std::path::Path,
    retention_timestamp: i64,
) -> Result<(usize, usize)> {
    use std::io::Write;

    let now = chrono::Local::now().format("%Y%m%d_%H%M%S");
    let gz_name = format!("detected_{now}.dump.gz");
    let gz_path = export_path.join(gz_name);
    let mut encoder = GzEncoder::new(std::fs::File::create(gz_path)?, Compression::default());

    let threshold_key = i128::from(retention_timestamp) << 64;
    let mut exported = 0;
    let mut failed = 0;
    for (key, event) in db.iter_forward().flatten() {
        if key >= threshold_key {
            break;
        }
        let r = match event {
            Event::DnsCovertChannel(x) => writeln!(encoder, "{x}"),
            Event::HttpThreat(x) => writeln!(encoder, "{x}"),
            Event::RdpBruteForce(x) => writeln!(encoder, "{x}"),
            Event::RepeatedHttpSessions(x) => writeln!(encoder, "{x}"),
            Event::TorConnection(x) => writeln!(encoder, "{x}"),
            Event::DomainGenerationAlgorithm(x) => writeln!(encoder, "{x}"),
            Event::FtpBruteForce(x) => writeln!(encoder, "{x}"),
            Event::FtpPlainText(x) => writeln!(encoder, "{x}"),
            Event::PortScan(x) => writeln!(encoder, "{x}"),
            Event::MultiHostPortScan(x) => writeln!(encoder, "{x}"),
            Event::ExternalDdos(x) => writeln!(encoder, "{x}"),
            Event::NonBrowser(x) => writeln!(encoder, "{x}"),
            Event::LdapBruteForce(x) => writeln!(encoder, "{x}"),
            Event::LdapPlainText(x) => writeln!(encoder, "{x}"),
            Event::CryptocurrencyMiningPool(x) => writeln!(encoder, "{x}"),
            Event::BlockList(record_type) => match record_type {
                RecordType::Conn(x) => writeln!(encoder, "{x}"),
                RecordType::Dns(x) => writeln!(encoder, "{x}"),
                RecordType::Http(x) => writeln!(encoder, "{x}"),
                RecordType::DceRpc(x) => writeln!(encoder, "{x}"),
                RecordType::Ftp(x) => writeln!(encoder, "{x}"),
                RecordType::Kerberos(x) => writeln!(encoder, "{x}"),
                RecordType::Ldap(x) => writeln!(encoder, "{x}"),
                RecordType::Mqtt(x) => writeln!(encoder, "{x}"),
                RecordType::Nfs(x) => writeln!(encoder, "{x}"),
                RecordType::Ntlm(x) => writeln!(encoder, "{x}"),
                RecordType::Rdp(x) => writeln!(encoder, "{x}"),
                RecordType::Smb(x) => writeln!(encoder, "{x}"),
                RecordType::Smtp(x) => writeln!(encoder, "{x}"),
                RecordType::Ssh(x) => writeln!(encoder, "{x}"),
                RecordType::Tls(x) => writeln!(encoder, "{x}"),
            },
        };
        if r.is_ok() {
            exported += 1;
        } else {
            failed += 1;
        }
    }

    encoder.finish()?;

    Ok((exported, failed))
}

#[cfg(test)]
mod tests {
    use crate::{event::DnsEventFields, EventKind, EventMessage, Store};
    use chrono::Utc;
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    fn example_message() -> EventMessage {
        let fields = DnsEventFields {
            source: "collector1".to_string(),
            session_end_time: Utc::now(),
            src_addr: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            src_port: 10000,
            dst_addr: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
            dst_port: 53,
            proto: 17,
            query: "foo.com".to_string(),
            answer: vec!["1.1.1.1".to_string()],
            trans_id: 1,
            rtt: 1,
            qclass: 0,
            qtype: 0,
            rcode: 0,
            aa_flag: false,
            tc_flag: false,
            rd_flag: false,
            ra_flag: false,
            ttl: vec![1; 5],
            confidence: 0.8,
        };
        EventMessage {
            time: Utc::now(),
            kind: EventKind::DnsCovertChannel,
            fields: bincode::serialize(&fields).expect("serializable"),
        }
    }

    #[tokio::test]
    async fn db_backup_list() {
        use crate::backup::list;
        use tokio::sync::RwLock;

        let db_dir = tempfile::tempdir().unwrap();
        let backup_dir = tempfile::tempdir().unwrap();

        let store = Arc::new(RwLock::new(
            Store::new(db_dir.path(), backup_dir.path()).unwrap(),
        ));

        {
            let store = store.read().await;
            let db = store.events();
            assert!(db.iter_forward().next().is_none());
        }

        let msg = example_message();

        // backing up 1
        {
            let mut store = store.write().await;
            let db = store.events();
            db.put(&msg).unwrap();
            let res = store.backup(true, 3);
            assert!(res.is_ok());
        }
        // backing up 2
        {
            let mut store = store.write().await;
            let db = store.events();
            db.put(&msg).unwrap();
            let res = store.backup(true, 3);
            assert!(res.is_ok());
        }

        // backing up 3
        {
            let mut store = store.write().await;
            let db = store.events();
            db.put(&msg).unwrap();
            let res = store.backup(true, 3);
            assert!(res.is_ok());
        }

        // get backup list
        let backup_list = list(&store).await.unwrap();
        assert_eq!(backup_list.len(), 3);
        assert_eq!(backup_list.get(0).unwrap().id, 1);
        assert_eq!(backup_list.get(1).unwrap().id, 2);
        assert_eq!(backup_list.get(2).unwrap().id, 3);
    }

    #[tokio::test]
    async fn db_retain() {
        use crate::backup::{export_events, retain_db};
        use std::fs;
        use tokio::sync::RwLock;

        let db_dir = tempfile::tempdir().unwrap();
        let backup_dir = tempfile::tempdir().unwrap();

        let export_dir = tempfile::tempdir().unwrap();

        let store = Arc::new(RwLock::new(
            Store::new(db_dir.path(), backup_dir.path()).unwrap(),
        ));

        {
            let store = store.read().await;
            let db = store.events();
            assert!(db.iter_forward().next().is_none());
        }

        let msg = example_message();

        // export events

        {
            let store = store.write().await;
            let db = store.events();
            db.put(&msg).unwrap();
        }
        let res = export_events(
            &store,
            export_dir.path(),
            Utc::now().timestamp_nanos_opt().unwrap(),
        )
        .await;
        assert!(res.is_ok());
        {
            let store = store.read().await;
            let db = store.events();
            assert!(db.iter_forward().next().is_some());
        }

        let res = retain_db(&store, Utc::now().timestamp_nanos_opt().unwrap()).await;
        assert!(res.is_ok());

        {
            let store = store.read().await;
            let db = store.events();
            assert!(db.iter_forward().next().is_none());
        }

        let entries: Vec<_> = fs::read_dir(export_dir.path()).unwrap().collect();
        assert_eq!(entries.len(), 1);

        let entry = &entries[0];
        let path = entry.as_ref().unwrap().path();
        assert_eq!(path.extension().and_then(|s| s.to_str()), Some("gz"));
    }
}
