//! Provides abstractions to use `AsyncRead` and `AsyncWrite` with
//! a [`WebSocketStream`](crate::WebSocketStream) or a [`WebSocketSender`](crate::WebSocketSender).

use std::{
    collections::VecDeque,
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

/// Policy that determines which websocket frames a [`ByteReader`] projects into
/// the byte stream.
///
/// Independent of the policy, a `Close` frame always ends the byte stream: once
/// it is observed, the current and all subsequent reads return EOF.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FramePolicy {
    /// Only binary messages are projected into the byte stream.
    ///
    /// Text messages and control frames (`Ping`, `Pong`, `Close`) are skipped.
    BinaryOnly,
    /// Text and binary messages are projected into the byte stream.
    ///
    /// Control frames (`Ping`, `Pong`, `Close`) are skipped and never leak into
    /// the payload. This is the default.
    #[default]
    TextAndBinary,
    /// Like [`TextAndBinary`](Self::TextAndBinary), but control frames (`Ping`,
    /// `Pong`, `Close`) are retained as events that can be retrieved in order
    /// with [`ByteReader::next_control_frame`].
    RetainControlFrames,
}

/// Treat a websocket [stream](Stream) as an `AsyncRead` implementation.
///
/// This also works with any other `Stream` of `Message`, such as a `SplitStream`.
///
/// Each read will only return data from one message. If you want to combine data from multiple
/// messages into one read, consider wrapping this in a `BufReader`.
///
/// Which frames are projected into the byte stream is controlled by the
/// [`FramePolicy`]. By default only text and binary messages contribute payload
/// bytes, and a `Close` frame (or the end of the underlying stream) makes all
/// subsequent reads return EOF.
#[derive(Debug)]
pub struct ByteReader<S> {
    stream: S,
    bytes: Option<Bytes>,
    policy: FramePolicy,
    control_frames: VecDeque<Message>,
    closed: bool,
}

impl<S> ByteReader<S> {
    /// Create a new `ByteReader` from a [stream](Stream) that returns a WebSocket [`Message`].
    ///
    /// This uses the default [`FramePolicy`]. See
    /// [`with_frame_policy`](Self::with_frame_policy) to select a different one.
    #[inline(always)]
    pub fn new(stream: S) -> Self {
        Self::with_frame_policy(stream, FramePolicy::default())
    }

    /// Create a new `ByteReader` from a [stream](Stream) that returns a WebSocket
    /// [`Message`], using the given [`FramePolicy`].
    #[inline(always)]
    pub fn with_frame_policy(stream: S, policy: FramePolicy) -> Self {
        Self {
            stream,
            bytes: None,
            policy,
            control_frames: VecDeque::new(),
            closed: false,
        }
    }

    /// Returns the [`FramePolicy`] used by this reader.
    #[inline(always)]
    pub fn frame_policy(&self) -> FramePolicy {
        self.policy
    }

    /// Returns the next retained control frame (`Ping`, `Pong` or `Close`), in
    /// the order the frames were received.
    ///
    /// Control frames are only retained when the reader was created with
    /// [`FramePolicy::RetainControlFrames`], otherwise this always returns
    /// `None`.
    #[inline(always)]
    pub fn next_control_frame(&mut self) -> Option<Message> {
        self.control_frames.pop_front()
    }

    /// Get the underlying [stream](Stream) back.
    ///
    /// Any partially read message and any retained control frames are discarded.
    #[inline(always)]
    pub fn into_inner(self) -> S {
        self.stream
    }

    fn retain_control_frame(&mut self, msg: Message) {
        if self.policy == FramePolicy::RetainControlFrames {
            self.control_frames.push_back(msg);
        }
    }
}

fn poll_read_helper<S>(
    s: Pin<&mut ByteReader<S>>,
    cx: &mut Context<'_>,
    buf_len: usize,
) -> Poll<io::Result<Option<Bytes>>>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    let me = s.get_mut();

    loop {
        // Always drain the remainder of the current message first so that data
        // from different messages is never mixed into a single read.
        if let Some(bytes) = me.bytes.take() {
            return Poll::Ready(Ok(Some(if bytes.len() > buf_len {
                me.bytes.insert(bytes).split_to(buf_len)
            } else {
                bytes
            })));
        }

        // Stable EOF: once the underlying stream ended or a `Close` frame was
        // observed, all subsequent polls return EOF without polling the
        // underlying stream again (which might never wake up again).
        if me.closed {
            return Poll::Ready(Ok(None));
        }

        match Pin::new(&mut me.stream).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => me.closed = true,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(convert_err(e))),
            Poll::Ready(Some(Ok(msg))) => match msg {
                Message::Binary(_) => me.bytes = Some(msg.into_data()),
                Message::Text(_) if me.policy != FramePolicy::BinaryOnly => {
                    me.bytes = Some(msg.into_data())
                }
                Message::Close(_) => {
                    me.retain_control_frame(msg);
                    me.closed = true;
                }
                // `Ping`, `Pong` and raw `Frame` messages are control events,
                // not payload.
                _ => me.retain_control_frame(msg),
            },
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
