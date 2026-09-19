//! Tests for the `ByteReader` frame policy and EOF semantics.

use async_tungstenite::{ByteReader, FramePolicy};
use futures::executor::block_on;
use futures::io::AsyncReadExt;
use futures::stream::{self, Stream};
use futures::StreamExt;
use tungstenite::{Error as WsError, Message};

fn message_stream(
    messages: Vec<Message>,
) -> impl Stream<Item = Result<Message, WsError>> + Unpin {
    stream::iter(messages.into_iter().map(Ok))
}

fn read_all<S>(reader: &mut ByteReader<S>) -> std::io::Result<Vec<u8>>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    let mut buf = Vec::new();
    block_on(reader.read_to_end(&mut buf))?;
    Ok(buf)
}

/// Read one more chunk and return how many bytes were read.
fn read_once<S>(reader: &mut ByteReader<S>, buf: &mut [u8]) -> std::io::Result<usize>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    block_on(reader.read(buf))
}

#[test]
fn default_policy_hides_control_frames_from_payload() {
    let mut reader = ByteReader::new(message_stream(vec![
        Message::Ping(b"ping-payload".to_vec().into()),
        Message::text("hello "),
        Message::Pong(b"pong-payload".to_vec().into()),
        Message::binary(b"world".to_vec()),
        Message::Close(None),
    ]));

    assert_eq!(reader.frame_policy(), FramePolicy::TextAndBinary);
    assert_eq!(read_all(&mut reader).unwrap(), b"hello world");
}

#[test]
fn close_gives_stable_eof_and_hides_later_messages() {
    let mut reader = ByteReader::new(message_stream(vec![
        Message::binary(b"data".to_vec()),
        Message::Close(None),
        // Must never be observed: the `Close` frame ends the byte stream.
        Message::text("unreachable"),
    ]));

    assert_eq!(read_all(&mut reader).unwrap(), b"data");

    // Every poll after the `Close` frame must keep returning EOF.
    let mut buf = [0u8; 16];
    for _ in 0..3 {
        assert_eq!(read_once(&mut reader, &mut buf).unwrap(), 0);
    }
}

#[test]
fn close_eof_is_stable_even_if_stream_would_block() {
    // After the `Close` frame the underlying stream never produces anything
    // again. Reads must return EOF instead of polling (and hanging on) it.
    let stream = message_stream(vec![Message::binary(b"data".to_vec()), Message::Close(None)])
        .chain(stream::pending());
    let mut reader = ByteReader::new(stream);

    assert_eq!(read_all(&mut reader).unwrap(), b"data");

    let mut buf = [0u8; 16];
    for _ in 0..3 {
        assert_eq!(read_once(&mut reader, &mut buf).unwrap(), 0);
    }
}

#[test]
fn binary_only_policy_skips_text_and_control_frames() {
    let mut reader = ByteReader::with_frame_policy(
        message_stream(vec![
            Message::text("skip me"),
            Message::binary(b"a".to_vec()),
            Message::Ping(b"ping".to_vec().into()),
            Message::text("skip me too"),
            Message::binary(b"b".to_vec()),
            Message::Close(None),
        ]),
        FramePolicy::BinaryOnly,
    );

    assert_eq!(read_all(&mut reader).unwrap(), b"ab");
}

#[test]
fn retain_control_frames_policy_collects_events_in_order() {
    let close = Message::Close(Some(tungstenite::protocol::CloseFrame {
        code: tungstenite::protocol::frame::coding::CloseCode::Normal,
        reason: "done".into(),
    }));
    let mut reader = ByteReader::with_frame_policy(
        message_stream(vec![
            Message::Ping(b"p1".to_vec().into()),
            Message::text("payload"),
            Message::Pong(b"p2".to_vec().into()),
            close.clone(),
            // Never observed: the stream ends at the `Close` frame.
            Message::binary(b"after-close".to_vec()),
        ]),
        FramePolicy::RetainControlFrames,
    );

    // Control frame payloads (including the close reason) must not leak into
    // the byte stream.
    assert_eq!(read_all(&mut reader).unwrap(), b"payload");

    assert_eq!(
        reader.next_control_frame(),
        Some(Message::Ping(b"p1".to_vec().into()))
    );
    assert_eq!(
        reader.next_control_frame(),
        Some(Message::Pong(b"p2".to_vec().into()))
    );
    assert_eq!(reader.next_control_frame(), Some(close));
    assert_eq!(reader.next_control_frame(), None);
}

#[test]
fn segmented_reads_never_mix_messages() {
    let mut reader = ByteReader::new(message_stream(vec![
        Message::binary(vec![b'a'; 10]),
        Message::Ping(b"interleaved".to_vec().into()),
        Message::text("bb"),
        Message::binary(b"ccc".to_vec()),
        Message::Close(None),
    ]));

    // Read with a buffer smaller than the first message: every chunk must come
    // from exactly one message, in order, without mixing or losing data.
    let mut chunks = Vec::new();
    let mut buf = [0u8; 4];
    loop {
        let n = read_once(&mut reader, &mut buf).unwrap();
        if n == 0 {
            break;
        }
        chunks.push(buf[..n].to_vec());
    }

    assert_eq!(
        chunks,
        vec![
            b"aaaa".to_vec(),
            b"aaaa".to_vec(),
            b"aa".to_vec(),
            b"bb".to_vec(),
            b"ccc".to_vec(),
        ]
    );
}

#[test]
fn errors_keep_diagnostic_context() {
    let stream = message_stream(vec![Message::text("a")]).chain(stream::iter(vec![Err(
        WsError::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake),
    )]));
    let mut reader = ByteReader::new(stream);

    let mut buf = [0u8; 8];
    assert_eq!(read_once(&mut reader, &mut buf).unwrap(), 1);
    assert_eq!(&buf[..1], b"a");

    let err = read_once(&mut reader, &mut buf).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::Other);
    let inner = err.into_inner().expect("error must keep its source");
    let ws_err = inner
        .downcast_ref::<WsError>()
        .expect("inner error must be the original tungstenite error");
    assert!(
        ws_err.to_string().contains("without closing handshake"),
        "unexpected error: {}",
        ws_err
    );
}

#[cfg(feature = "tokio-runtime")]
#[test]
fn tokio_entry_point_matches_futures_io() {
    let mut reader = ByteReader::new(message_stream(vec![
        Message::Ping(b"ping".to_vec().into()),
        Message::binary(b"ab".to_vec()),
        Message::Close(None),
        Message::binary(b"unreachable".to_vec()),
    ]));

    let mut buf = [0u8; 8];
    // Small buffer forces segmentation through the tokio `AsyncRead` impl.
    let n = block_on(tokio::io::AsyncReadExt::read(&mut reader, &mut buf[..1])).unwrap();
    assert_eq!(n, 1);
    assert_eq!(&buf[..1], b"a");
    let n = block_on(tokio::io::AsyncReadExt::read(&mut reader, &mut buf)).unwrap();
    assert_eq!(n, 1);
    assert_eq!(&buf[..1], b"b");
    // Stable EOF after the `Close` frame.
    for _ in 0..3 {
        assert_eq!(
            block_on(tokio::io::AsyncReadExt::read(&mut reader, &mut buf)).unwrap(),
            0
        );
    }
}

/// Reads a `ByteReader` to EOF in 7-byte chunks and additionally verifies
/// that EOF stays stable afterwards.
#[cfg(feature = "async-std-runtime")]
async fn read_in_small_chunks<S>(reader: &mut ByteReader<S>) -> Vec<u8>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    let mut out = Vec::new();
    let mut buf = [0u8; 7];
    loop {
        let n = reader.read(&mut buf).await.expect("read failed");
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    for _ in 0..3 {
        assert_eq!(reader.read(&mut buf).await.expect("read failed"), 0);
    }
    out
}

#[cfg(feature = "async-std-runtime")]
async fn run_client<S>(stream: async_tungstenite::WebSocketStream<S>, split: bool) -> Vec<u8>
where
    S: futures::AsyncRead + futures::AsyncWrite + Unpin,
{
    if split {
        let (_sender, receiver) = stream.split();
        let mut reader = ByteReader::new(receiver);
        read_in_small_chunks(&mut reader).await
    } else {
        let mut reader = ByteReader::new(stream);
        read_in_small_chunks(&mut reader).await
    }
}

#[cfg(feature = "async-std-runtime")]
async fn roundtrip(split: bool) -> Vec<u8> {
    use async_std::net::{TcpListener, TcpStream};
    use async_std::task;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = task::spawn(async move {
        let (connection, _) = listener.accept().await.unwrap();
        let mut stream = async_tungstenite::accept_async(connection)
            .await
            .unwrap();
        stream.send(Message::text("hello ")).await.unwrap();
        stream
            .send(Message::binary(
                (0..4096u32).map(|i| (i % 251) as u8).collect::<Vec<_>>(),
            ))
            .await
            .unwrap();
        stream
            .send(Message::Ping(b"control".to_vec().into()))
            .await
            .unwrap();
        stream.close(None).await.unwrap();
    });

    let tcp = TcpStream::connect(addr).await.unwrap();
    let (stream, _) = async_tungstenite::client_async("ws://localhost/", tcp)
        .await
        .unwrap();
    let received = run_client(stream, split).await;
    server.await;
    received
}

#[cfg(feature = "async-std-runtime")]
#[test]
fn split_and_unsplit_read_identically() {
    let mut expected = b"hello ".to_vec();
    expected.extend((0..4096u32).map(|i| (i % 251) as u8));

    let unsplit = async_std::task::block_on(roundtrip(false));
    assert_eq!(unsplit, expected);

    let split = async_std::task::block_on(roundtrip(true));
    assert_eq!(split, expected);
    assert_eq!(split, unsplit);
}
