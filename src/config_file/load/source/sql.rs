use super::super::*;
use super::SourceSpec;

/// A `postgres` and a `mysql` source are one struct, told apart by its `kind`.
impl SourceSpec for SqlSourceDto {
    fn check(&self, s: &SourceDto, errors: &mut ItemErrors<'_>) {
        let (kind, sql) = (self.kind, self);
        let SqlSourceDto {
            host,
            port,
            database,
            user,
            encrypted,
            tls,
            query,
            mode,
            fields,
            poll_interval_secs,
            batch,
            ..
        } = sql;
        let what = format!("source {:?}", s.id);
        if tls.is_set() && *encrypted == SqlTlsDto::Disable {
            errors.at("certificate").push(format!(
                "{what} sets TLS and turns it off, so nothing its certificate or trusted authorities say would be \
                         used. Set tls to \"require\", or take them off it"
            ));
        }
        for (field, value, names) in [
            ("host", host, "the database server's host name or address"),
            ("database", database, "the database the query runs in"),
            ("user", user, "the user it connects as"),
        ] {
            if value.trim().is_empty() {
                errors.at(field).push(format!("{what} names no {field}; give {names}"));
            }
        }
        if *port == Some(0) {
            errors
                .at("port")
                .push(format!("{what} connects to port 0; give the server's port"));
        }
        let read = SqlRead {
            query,
            mode,
            fields,
            poll_interval_secs: *poll_interval_secs,
            batch: *batch,
        };
        read.check(&s.id, kind.dialect(), kind.meta(), errors);
    }

    fn to_kind(&self, of: &str) -> SourceKind {
        let (kind, sql) = (self.kind, self);
        let database = sql_database(sql, of);
        let topic = database.address();
        let read = SqlRead {
            query: &sql.query,
            mode: &sql.mode,
            fields: &sql.fields,
            poll_interval_secs: sql.poll_interval_secs,
            batch: sql.batch,
        };
        SourceKind::Sql(read.config(
            kind.dialect(),
            kind.meta(),
            &format!("{}://{topic}", kind.scheme()),
            topic.clone(),
            database,
        ))
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        self.password.iter().map(|r| ("password", r)).collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        self.password.iter_mut().collect()
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        Vec::new()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn declared_meta(&self) -> SourceMeta {
        self.meta(self.kind)
    }

    fn single_worker(&self) -> bool {
        true
    }
}
