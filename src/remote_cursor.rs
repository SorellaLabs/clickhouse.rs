use crate::DbRow;
use crate::{error::Error, rowbinary};
use crate::{response::Chunks, Compression};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use hyper::Body;
use serde::Deserialize;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::buflist::BufList;

/// A cursor for deserializing using the row binary format from a byte buffer.
pub struct RemoteCursor<T, S> {
    stream: S,
    pending: BufList<Bytes>,
    tmp_buf: Vec<u8>,
    _p: PhantomData<T>,
}

impl<T, S> RemoteCursor<T, S>
where
    S: Stream<Item = reqwest::Result<Bytes>>,
    T: DbRow + for<'b> Deserialize<'b>,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            tmp_buf: vec![0; 1024],
            pending: BufList::default(),
            _p: Default::default(),
        }
    }
}

impl<T, S> Stream for RemoteCursor<T, S>
where
    S: Stream<Item = reqwest::Result<Bytes>> + Unpin,
    T: DbRow + for<'b> Deserialize<'b> + Unpin,
{
    type Item = crate::Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match rowbinary::deserialize_from(&mut this.pending, &mut this.tmp_buf[..]) {
                Ok(value) => {
                    this.pending.commit();
                    return Poll::Ready(Some(Ok(value)));
                }
                Err(Error::TooSmallBuffer(need)) => {
                    let new_len = (this.tmp_buf.len() + need)
                        .checked_next_power_of_two()
                        .expect("oom");
                    this.tmp_buf.resize(new_len, 0);

                    this.pending.rollback();
                    continue;
                }
                Err(Error::NotEnoughData) => {
                    this.pending.rollback();
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }

            match this.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(v)) => match v {
                    Ok(val) => {
                        this.pending.push(val);
                    }
                    Err(e) => return Poll::Ready(Some(Err(Error::Custom(e.to_string())))),
                },
                Poll::Ready(None) if this.pending.bufs_cnt() > 0 => {
                    return Poll::Ready(Some(Err(Error::NotEnoughData)));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
