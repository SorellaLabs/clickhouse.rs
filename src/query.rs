use bytes::BufMut;
use futures::StreamExt;
use hyper::{header::CONTENT_LENGTH, Body, Method, Request};
use serde::Deserialize;
use url::Url;

use crate::{
    cursor::RowBinaryCursor,
    error::{Error, Result},
    response::{Chunks, Response},
    row::DbRow,
    sql::{Bind, SqlBuilder},
    Client,
};

const MAX_QUERY_LEN_TO_USE_GET: usize = 8192;

#[must_use]
#[derive(Clone)]
pub struct Query {
    client: Client,
    sql: SqlBuilder,
}

impl Query
where
    Self: Send + Sync,
{
    pub(crate) fn new(client: &Client, template: &str) -> Self {
        Self {
            client: client.clone(),
            sql: SqlBuilder::new(template),
        }
    }

    /// Binds `value` to the next `?` in the query.
    ///
    /// The `value`, which must either implement [`Serialize`](serde::Serialize)
    /// or be an [`Identifier`], will be appropriately escaped.
    ///
    /// WARNING: This means that the query must not have any extra `?`, even if
    /// they are in a string literal!
    pub fn bind(mut self, value: impl Bind) -> Self {
        self.sql.bind_arg(value);
        self
    }

    /// Executes the query.
    pub async fn execute(self) -> Result<()> {
        self.do_execute(false)?.finish().await
    }

    /// Executes the query, returning a [`RowCursor`] to obtain results.
    ///
    /// # Example
    ///
    /// ```
    /// # async fn example() -> clickhouse::error::Result<()> {
    /// #[derive(clickhouse::Row, serde::Deserialize)]
    /// struct MyRow<'a> {
    ///     no: u32,
    ///     name: &'a str,
    /// }
    ///
    /// let mut cursor = clickhouse::Client::default()
    ///     .query("SELECT ?fields FROM some WHERE no BETWEEN 0 AND 1")
    ///     .fetch::<MyRow<'_>>()?;
    ///
    /// while let Some(MyRow { name, no }) = cursor.next().await? {
    ///     println!("{name}: {no}");
    /// }
    /// # Ok(()) }
    /// ```
    pub fn fetch<T>(mut self) -> Result<RowCursor<T>>
    where
        T: DbRow + for<'b> Deserialize<'b>,
    {
        self.sql.bind_fields::<T>();
        self.sql.append(" FORMAT RowBinary");

        let response = self.do_execute(true)?;
        Ok(RowCursor(RowBinaryCursor::new(response)))
    }

    /// Executes the query and returns just a single row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_one<T>(self) -> Result<T>
    where
        T: DbRow + for<'b> Deserialize<'b> + Send,
    {
        match self.fetch()?.next().await {
            Ok(Some(row)) => Ok(row),
            Ok(None) => Err(Error::RowNotFound),
            Err(err) => Err(err),
        }
    }

    /// Executes the query and returns at most one row.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_optional<T>(self) -> Result<Option<T>>
    where
        T: DbRow + for<'b> Deserialize<'b> + Send,
    {
        self.fetch()?.next().await
    }

    /// Executes the query and returns all the generated results, collected into a Vec.
    ///
    /// Note that `T` must be owned.
    pub async fn fetch_all<T>(self) -> Result<Vec<T>>
    where
        T: DbRow + for<'b> Deserialize<'b> + Send,
    {
        let mut result = Vec::new();
        let mut cursor = self.fetch::<T>()?;

        while let Some(row) = cursor.next().await? {
            result.push(row);
        }

        Ok(result)
    }

    /// Executes the query and returns the bytes from clickhouse.
    /// This returns a result as when processing the bytes. we look at them and check
    /// for clickhouse errors.
    pub async fn fetch_raw<T>(mut self) -> Result<Vec<u8>>
    where
        T: DbRow + for<'b> Deserialize<'b>,
    {
        self.sql.bind_fields::<T>();
        self.sql.append(" FORMAT RowBinary");

        let mut res = self.do_execute(true)?;
        let chunks = res.chunks_slow().await?;

        let mut result = Vec::new();
        while let Some(next) = chunks.next().await {
            result.put(next?);
        }

        Ok(result)
    }

    pub(crate) fn do_execute(self, read_only: bool) -> Result<Response> {
        let query = self.sql.finish()?;

        let mut url =
            Url::parse(&self.client.url).map_err(|err| Error::InvalidParams(Box::new(err)))?;
        let mut pairs = url.query_pairs_mut();
        pairs.clear();

        if let Some(database) = &self.client.database {
            pairs.append_pair("database", database);
        }

        let use_post = !read_only || query.len() > MAX_QUERY_LEN_TO_USE_GET;
        let method = if use_post { Method::POST } else { Method::GET };

        let (body, content_length) = if use_post {
            if read_only {
                pairs.append_pair("readonly", "1");
            }
            let len = query.len();
            (Body::from(query), len)
        } else {
            pairs.append_pair("query", &query);
            (Body::empty(), 0)
        };

        if self.client.compression.is_lz4() {
            pairs.append_pair("compress", "1");
        }

        for (name, value) in &self.client.options {
            pairs.append_pair(name, value);
        }
        drop(pairs);

        let mut builder = Request::builder().method(method).uri(url.as_str());

        if content_length == 0 {
            builder = builder.header(CONTENT_LENGTH, "0");
        } else {
            builder = builder.header(CONTENT_LENGTH, content_length.to_string());
        }

        if let Some(user) = &self.client.user {
            builder = builder.header("X-ClickHouse-User", user);
        }

        if let Some(password) = &self.client.password {
            builder = builder.header("X-ClickHouse-Key", password);
        }

        let request = builder
            .body(body)
            .map_err(|err| Error::InvalidParams(Box::new(err)))?;

        let future = self.client.client._request(request);
        Ok(Response::new(future, self.client.compression))
    }
}

/// A cursor that emits rows.
pub struct RowCursor<T>(RowBinaryCursor<T>);

impl<T> RowCursor<T>
where
    Self: Send,
    T: Send,
{
    /// Emits the next row.
    pub async fn next<'a, 'b: 'a>(&'a mut self) -> Result<Option<T>>
    where
        T: Deserialize<'b>,
    {
        self.0.next().await
    }
}
