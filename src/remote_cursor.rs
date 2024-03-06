use crate::rowbinary;
use crate::{response::Chunks, Compression};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use hyper::Body;
use serde::Deserialize;
use std::marker::PhantomData;
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
    S: Stream<Item = Result<Bytes>>,
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
    S: Stream<Item = Result<Bytes>> + Unpin,
    T: DbRow + for<'b> Deserialize<'b> + Unpin,
{
    type Item = crate::Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let tmp_buf = &mut self.tmp_buf;
        loop {
            match rowbinary::deserialize_from(&mut self.pending, &mut temp_buf[..]) {
                Ok(value) => {
                    self.pending.commit();
                    return Poll::Ready(Some(Ok(value)));
                }
                Err(Error::TooSmallBuffer(need)) => {
                    let new_len = (tmp_buf.len() + need)
                        .checked_next_power_of_two()
                        .expect("oom");
                    tmp_buf.resize(new_len, 0);

                    self.pending.rollback();
                    continue;
                }
                Err(Error::NotEnoughData) => {
                    self.pending.rollback();
                }
                Err(e) => return Poll::Ready(Some(Err(e))),
            }

            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(v)) => match v {
                    Ok(val) => {
                        self.pending.push(val);
                    }
                    Err(e) => return Poll::Ready(Some(Err(e))),
                },
                Poll::Ready(None) if self.pending.bufs_cnt() > 0 => {
                    return Poll::Ready(Some(Err(Error::NotEnoughData)));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
