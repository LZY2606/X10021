//! Provides abstractions to use `AsyncRead` and `AsyncWrite` with
//! a [`WebSocketStream`](crate::WebSocketStream) or a [`WebSocketSender`](crate::WebSocketSender).

use std::{
    fmt, io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::stream::Stream;

use crate::{tungstenite::Bytes, Message, WsError};

/// Treat a websocket [sender](Sender) as an `AsyncWrite` implementation.
///
/// Every write sends a binary message. If you want to group writes together, consider wrapping
/// this with a `BufWriter`.
pub struct ByteWriter<S> {
    sender: S,
    state: State,
}

impl<S> ByteWriter<S> {
    /// Create a new `ByteWriter` from a [sender](Sender) that accepts a websocket [`Message`].
    #[inline(always)]
    pub fn new(sender: S) -> Self
    where
        S: Sender,
    {
        Self {
            sender,
            state: State::Open,
        }
    }

    /// Get the underlying [sender](Sender) back.
    #[inline(always)]
    pub fn into_inner(self) -> S {
        self.sender
    }
}

impl<S> fmt::Debug for ByteWriter<S>
where
    S: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteWriter")
            .field("sender", &self.sender)
            .field("state", &"..")
            .finish()
    }
}

enum State {
    Open,
    Closing(Option<Message>),
}

impl State {
    fn close(&mut self) -> &mut Option<Message> {
        match self {
            State::Open => {
                *self = State::Closing(Some(Message::Close(None)));
                if let State::Closing(msg) = self {
                    msg
                } else {
                    unreachable!()
                }
            }
            State::Closing(msg) => msg,
        }
    }
}

/// Sends bytes as a websocket [`Message`].
///
/// It's implemented for [`WebSocketStream`](crate::WebSocketStream)
/// and [`WebSocketSender`](crate::WebSocketSender).
/// It's also implemeted for every `Sink` type that accepts
/// a websocket [`Message`] and returns [`WsError`] type as
/// an error when `futures-03-sink` feature is enabled.
pub trait Sender: private::SealedSender {}

pub(crate) mod private {
    use super::*;

    pub trait SealedSender {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, WsError>>;

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>>;

        fn poll_close(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            msg: &mut Option<Message>,
        ) -> Poll<Result<(), WsError>>;
    }

    impl<S> Sender for S where S: SealedSender {}
}

#[cfg(feature = "futures-03-sink")]
impl<S> private::SealedSender for S
where
    S: futures_util::Sink<Message, Error = WsError> + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, WsError>> {
        use std::task::ready;

        ready!(self.as_mut().poll_ready(cx))?;
        let len = buf.len();
        self.start_send(Message::binary(buf.to_owned()))?;
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        <S as futures_util::Sink<_>>::poll_flush(self, cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        _: &mut Option<Message>,
    ) -> Poll<Result<(), WsError>> {
        <S as futures_util::Sink<_>>::poll_close(self, cx)
    }
}

impl<S> futures_io::AsyncWrite for ByteWriter<S>
where
    S: Sender + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <S as private::SealedSender>::poll_write(Pin::new(&mut self.sender), cx, buf)
            .map_err(convert_err)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <S as private::SealedSender>::poll_flush(Pin::new(&mut self.sender), cx)
            .map_err(convert_err)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let msg = me.state.close();
        <S as private::SealedSender>::poll_close(Pin::new(&mut me.sender), cx, msg)
            .map_err(convert_err)
    }
}

#[cfg(feature = "tokio-runtime")]
impl<S> tokio::io::AsyncWrite for ByteWriter<S>
where
    S: Sender + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        <S as private::SealedSender>::poll_write(Pin::new(&mut self.sender), cx, buf)
            .map_err(convert_err)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        <S as private::SealedSender>::poll_flush(Pin::new(&mut self.sender), cx)
            .map_err(convert_err)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let msg = me.state.close();
        <S as private::SealedSender>::poll_close(Pin::new(&mut me.sender), cx, msg)
            .map_err(convert_err)
    }
}

/// Treat a websocket [stream](Stream) as an `AsyncRead` implementation.
///
/// This also works with any other `Stream` of `Message`, such as a `SplitStream`.
///
/// Each read will only return data from one message. If you want to combine data from multiple
/// messages into one read, consider wrapping this in a `BufReader`.
///
/// Which messages are projected into the byte stream is controlled by the [`FramePolicy`].
/// The default policy ([`FramePolicy::TextAndBinary`]) only exposes `Text` and `Binary`
/// payloads: `Ping`/`Pong` control frames are consumed but never surface as bytes, and a
/// `Close` frame — like the end of the underlying stream — terminates the byte stream.
/// Once EOF has been reached, every subsequent read returns EOF again without polling the
/// underlying stream any further.
#[derive(Debug)]
pub struct ByteReader<S> {
    stream: S,
    bytes: Option<Bytes>,
    policy: FramePolicy,
    closed: bool,
}

impl<S> ByteReader<S> {
    /// Create a new `ByteReader` from a [stream](Stream) that returns a WebSocket [`Message`].
    ///
    /// This uses the default [`FramePolicy`], see [`FramePolicy::default`].
    #[inline(always)]
    pub fn new(stream: S) -> Self {
        Self::with_policy(stream, FramePolicy::default())
    }

    /// Create a new `ByteReader` with an explicit [`FramePolicy`].
    #[inline(always)]
    pub fn with_policy(stream: S, policy: FramePolicy) -> Self {
        Self {
            stream,
            bytes: None,
            policy,
            closed: false,
        }
    }

    /// Get the current [`FramePolicy`].
    #[inline(always)]
    pub fn policy(&self) -> FramePolicy {
        self.policy
    }

    /// Change the [`FramePolicy`].
    ///
    /// This only affects messages that have not been consumed yet: bytes that are
    /// already buffered from a partially read message are still returned first.
    #[inline(always)]
    pub fn set_policy(&mut self, policy: FramePolicy) {
        self.policy = policy;
    }

    /// Get the underlying [stream](Stream) back.
    #[inline(always)]
    pub fn into_inner(self) -> S {
        self.stream
    }
}

/// Policy deciding which WebSocket frames a [`ByteReader`] projects into the byte stream.
///
/// Independent of the policy, a `Close` frame always terminates the byte stream with a
/// stable EOF: once EOF was returned, all further reads return EOF again without polling
/// the underlying stream any further. Raw [`Message::Frame`] messages are always treated
/// as payload, and messages with an empty payload are skipped so that they can not be
/// mistaken for EOF.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FramePolicy {
    /// Only `Binary` messages contribute bytes. `Text` messages and `Ping`/`Pong`
    /// control frames are consumed but skipped.
    Binary,
    /// `Text` and `Binary` messages contribute bytes. `Ping`/`Pong` control frames are
    /// consumed but skipped. This is the default.
    #[default]
    TextAndBinary,
    /// `Text` and `Binary` messages as well as the payloads of `Ping`/`Pong` control
    /// frames contribute bytes.
    All,
}

/// How a single message is handled under a [`FramePolicy`].
enum FrameProjection {
    /// The message payload becomes part of the byte stream.
    Payload(Bytes),
    /// The message is consumed but does not surface in the byte stream.
    Skip,
    /// The message terminates the byte stream.
    Close,
}

impl FramePolicy {
    fn project(self, msg: Message) -> FrameProjection {
        match msg {
            Message::Close(_) => FrameProjection::Close,
            Message::Ping(_) | Message::Pong(_) if self != FramePolicy::All => {
                FrameProjection::Skip
            }
            Message::Text(_) if self == FramePolicy::Binary => FrameProjection::Skip,
            msg => {
                let bytes = msg.into_data();
                // Empty payloads must not surface as a 0-byte read, which would be
                // indistinguishable from EOF for the caller.
                if bytes.is_empty() {
                    FrameProjection::Skip
                } else {
                    FrameProjection::Payload(bytes)
                }
            }
        }
    }
}

fn poll_read_helper<S>(
    mut s: Pin<&mut ByteReader<S>>,
    cx: &mut Context<'_>,
    buf_len: usize,
) -> Poll<io::Result<Option<Bytes>>>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    // A 0-byte read must not consume messages or be mistaken for EOF.
    if buf_len == 0 {
        return Poll::Ready(Ok(Some(Bytes::new())));
    }

    // Once the stream was closed, either by a `Close` frame or by the underlying stream
    // ending, every further read returns EOF without polling the stream again.
    if s.closed {
        return Poll::Ready(Ok(None));
    }

    loop {
        if let Some(mut bytes) = s.bytes.take() {
            let out = if bytes.len() > buf_len {
                let head = bytes.split_to(buf_len);
                s.bytes = Some(bytes);
                head
            } else {
                bytes
            };
            return Poll::Ready(Ok(Some(out)));
        }

        let msg = match Pin::new(&mut s.stream).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                s.closed = true;
                return Poll::Ready(Ok(None));
            }
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(convert_err(e))),
            Poll::Ready(Some(Ok(msg))) => msg,
        };

        match s.policy.project(msg) {
            FrameProjection::Skip => continue,
            FrameProjection::Close => {
                s.closed = true;
                return Poll::Ready(Ok(None));
            }
            FrameProjection::Payload(bytes) => {
                let out = if bytes.len() > buf_len {
                    s.bytes.insert(bytes).split_to(buf_len)
                } else {
                    bytes
                };
                return Poll::Ready(Ok(Some(out)));
            }
        }
    }
}

impl<S> futures_io::AsyncRead for ByteReader<S>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        poll_read_helper(self, cx, buf.len()).map_ok(|bytes| {
            bytes.map_or(0, |bytes| {
                buf[..bytes.len()].copy_from_slice(&bytes);
                bytes.len()
            })
        })
    }
}

#[cfg(feature = "tokio-runtime")]
impl<S> tokio::io::AsyncRead for ByteReader<S>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf,
    ) -> Poll<io::Result<()>> {
        poll_read_helper(self, cx, buf.remaining()).map_ok(|bytes| {
            if let Some(ref bytes) = bytes {
                buf.put_slice(bytes);
            }
        })
    }
}

fn convert_err(e: WsError) -> io::Error {
    match e {
        WsError::Io(io) => io,
        _ => io::Error::new(io::ErrorKind::Other, e),
    }
}
