//! Tests for the `ByteReader` frame policy and EOF semantics.
//!
//! Each test is self-contained and can be run individually, e.g.
//! `cargo test --features async-std-runtime --test byte_reader close_frame_yields_stable_eof`.

use async_tungstenite::{ByteReader, FramePolicy, WebSocketStream};
use futures::channel::mpsc;
use futures::io::AsyncReadExt as _;
use futures::stream::{self, Stream};
use tungstenite::{protocol::Role, Bytes, Error as WsError, Message};

const READ_BUF_LEN: usize = 999;

fn msg_stream(msgs: Vec<Message>) -> impl Stream<Item = Result<Message, WsError>> + Unpin {
    stream::iter(msgs.into_iter().map(Ok))
}

/// Read until EOF with a small, fixed buffer and assert that EOF is stable,
/// i.e. further reads keep returning 0 bytes.
async fn read_all<S>(reader: &mut ByteReader<S>) -> Vec<u8>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    let mut out = Vec::new();
    let mut buf = [0u8; READ_BUF_LEN];
    loop {
        let n = reader.read(&mut buf).await.expect("read failed");
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    // EOF must be stable: polling again must not yield more data or an error.
    for _ in 0..3 {
        assert_eq!(
            reader.read(&mut buf).await.expect("read after EOF failed"),
            0,
            "read after EOF must keep returning 0 bytes"
        );
    }
    out
}

#[test]
fn default_policy_hides_control_frames() {
    let msgs = vec![
        Message::text("a"),
        Message::Ping(Bytes::from_static(b"ping")),
        Message::binary("b"),
        Message::Pong(Bytes::from_static(b"pong")),
        Message::text("c"),
        Message::Close(None),
    ];
    let mut reader = ByteReader::new(msg_stream(msgs));
    assert_eq!(reader.policy(), FramePolicy::TextAndBinary);
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"abc", "control frames must not leak into the payload");
}

#[test]
fn binary_policy_skips_text_and_control() {
    let msgs = vec![
        Message::text("skip"),
        Message::Ping(Bytes::from_static(b"skip")),
        Message::binary("a"),
        Message::Pong(Bytes::from_static(b"skip")),
        Message::text("skip"),
        Message::binary("b"),
        Message::Close(None),
    ];
    let mut reader = ByteReader::with_policy(msg_stream(msgs), FramePolicy::Binary);
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"ab");
}

#[test]
fn all_policy_keeps_control_payloads() {
    let msgs = vec![
        Message::Ping(Bytes::from_static(b"p1")),
        Message::text("t"),
        Message::Pong(Bytes::from_static(b"p2")),
        Message::binary("b"),
        Message::Close(None),
    ];
    let mut reader = ByteReader::with_policy(msg_stream(msgs), FramePolicy::All);
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"p1tp2b");
}

#[test]
fn set_policy_takes_effect_for_unread_messages() {
    let (mut tx, rx) = mpsc::channel::<Result<Message, WsError>>(8);
    tx.try_send(Ok(Message::Ping(Bytes::from_static(b"x"))))
        .unwrap();
    tx.try_send(Ok(Message::binary("y"))).unwrap();
    tx.try_send(Ok(Message::Close(None))).unwrap();

    let mut reader = ByteReader::new(rx);
    // Under the default policy the ping is invisible.
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"y");

    // Switching policy only affects messages that have not been consumed yet.
    let (mut tx, rx) = mpsc::channel::<Result<Message, WsError>>(8);
    tx.try_send(Ok(Message::Ping(Bytes::from_static(b"x"))))
        .unwrap();
    tx.try_send(Ok(Message::binary("y"))).unwrap();
    tx.try_send(Ok(Message::Close(None))).unwrap();
    let mut reader = ByteReader::new(rx);
    reader.set_policy(FramePolicy::All);
    assert_eq!(reader.policy(), FramePolicy::All);
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"xy");
}

#[test]
fn close_frame_yields_stable_eof() {
    smol::block_on(async {
        let (mut tx, rx) = mpsc::channel::<Result<Message, WsError>>(8);
        let mut reader = ByteReader::new(rx);
        let mut buf = [0u8; 16];

        tx.try_send(Ok(Message::binary("hi"))).unwrap();
        assert_eq!(reader.read(&mut buf).await.unwrap(), 2);

        tx.try_send(Ok(Message::Close(None))).unwrap();
        assert_eq!(reader.read(&mut buf).await.unwrap(), 0, "close must be EOF");

        // Even if more messages arrive after the close, every further poll must
        // keep returning EOF instead of coming back to life.
        tx.try_send(Ok(Message::binary("late"))).unwrap();
        for _ in 0..3 {
            assert_eq!(
                reader.read(&mut buf).await.unwrap(),
                0,
                "EOF after close must be stable"
            );
        }
    });
}

#[test]
fn chunked_reads_never_mix_messages() {
    let first = vec![0xAA; 5_000];
    let second = vec![0xBB; 3_000];
    let msgs = vec![
        Message::binary(first.clone()),
        Message::binary(second.clone()),
        Message::Close(None),
    ];
    let mut reader = ByteReader::new(msg_stream(msgs));

    smol::block_on(async {
        let mut out = Vec::new();
        let mut buf = [0u8; 1_024];
        loop {
            let n = reader.read(&mut buf).await.unwrap();
            if n == 0 {
                break;
            }
            let chunk = &buf[..n];
            assert!(
                chunk.iter().all(|&b| b == chunk[0]),
                "a single read must never mix bytes from two messages"
            );
            out.extend_from_slice(chunk);
        }
        let mut expected = first;
        expected.extend_from_slice(&second);
        assert_eq!(
            out, expected,
            "chunked reads must preserve order and content"
        );
    });
}

#[test]
fn empty_messages_are_not_eof() {
    let msgs = vec![
        Message::binary(""),
        Message::text(""),
        Message::Ping(Bytes::new()),
        Message::binary("x"),
        Message::Close(None),
    ];
    let mut reader = ByteReader::with_policy(msg_stream(msgs), FramePolicy::All);
    let out = smol::block_on(read_all(&mut reader));
    assert_eq!(out, b"x", "empty payloads must not be mistaken for EOF");
}

#[test]
fn zero_len_read_buffer_consumes_nothing() {
    smol::block_on(async {
        let mut reader = ByteReader::new(msg_stream(vec![
            Message::binary("ab"),
            Message::Close(None),
        ]));
        assert_eq!(reader.read(&mut []).await.unwrap(), 0);
        // The empty read above must not have consumed the message or signalled EOF.
        let mut buf = [0u8; 8];
        assert_eq!(reader.read(&mut buf).await.unwrap(), 2);
        assert_eq!(&buf[..2], b"ab");
    });
}

fn wire_messages() -> Vec<Message> {
    vec![
        Message::text("hello "),
        Message::Ping(Bytes::from_static(b"heartbeat")),
        Message::binary(vec![0xAB; 70_000]),
        Message::text("done"),
    ]
}

fn expected_wire_payload() -> Vec<u8> {
    let mut expected = b"hello ".to_vec();
    expected.extend_from_slice(&[0xAB; 70_000]);
    expected.extend_from_slice(b"done");
    expected
}

/// Run an in-memory client/server pair and collect everything a `ByteReader`
/// with the default policy produces on the server side. When `split` is set,
/// the server stream is split into sender/receiver halves first.
async fn read_wire_payload(split: bool) -> Vec<u8> {
    // In-memory connection over the loopback interface with an ephemeral port:
    // no external network, no fixed ports, no sleeps.
    let listener = smol::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind failed");
    let addr = listener.local_addr().expect("local addr");

    let client = async {
        let tcp = smol::net::TcpStream::connect(addr).await.expect("connect");
        let mut ws = WebSocketStream::from_raw_socket(tcp, Role::Client, None).await;
        for msg in wire_messages() {
            ws.send(msg).await.expect("client send failed");
        }
        ws.close(None).await.expect("client close failed");
    };

    let server = async {
        let (tcp, _) = listener.accept().await.expect("accept");
        let ws = WebSocketStream::from_raw_socket(tcp, Role::Server, None).await;
        if split {
            let (tx, rx) = ws.split();
            let mut reader = ByteReader::new(rx);
            let out = read_all(&mut reader).await;
            drop(tx);
            out
        } else {
            let mut reader = ByteReader::new(ws);
            read_all(&mut reader).await
        }
    };

    let ((), out) = futures::join!(client, server);
    out
}

#[test]
fn split_and_unsplit_read_identically() {
    let unsplit = smol::block_on(read_wire_payload(false));
    assert_eq!(
        unsplit,
        expected_wire_payload(),
        "unsplit stream: ping payload must not leak, close must terminate the byte stream"
    );

    let split = smol::block_on(read_wire_payload(true));
    assert_eq!(
        split, unsplit,
        "split sender/receiver must observe the same byte stream as the unsplit stream"
    );
}

/// The tokio `AsyncRead` entry point must produce the exact same chunk
/// boundaries and EOF semantics as the futures-io one.
#[cfg(feature = "tokio-runtime")]
#[test]
fn tokio_read_matches_futures_io() {
    fn msgs() -> Vec<Message> {
        vec![
            Message::text("hello "),
            Message::Ping(Bytes::from_static(b"heartbeat")),
            Message::binary(vec![0xCD; 10_000]),
            Message::Close(None),
            // Must never be observed: EOF after close is stable.
            Message::binary("late"),
        ]
    }

    async fn chunked_reads_futures(
        reader: &mut ByteReader<impl Stream<Item = Result<Message, WsError>> + Unpin>,
    ) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        let mut buf = [0u8; 512];
        loop {
            let n = futures::io::AsyncReadExt::read(reader, &mut buf)
                .await
                .unwrap();
            if n == 0 {
                break;
            }
            chunks.push(buf[..n].to_vec());
        }
        // EOF is stable.
        assert_eq!(
            futures::io::AsyncReadExt::read(reader, &mut buf)
                .await
                .unwrap(),
            0
        );
        chunks
    }

    async fn chunked_reads_tokio(
        reader: &mut ByteReader<impl Stream<Item = Result<Message, WsError>> + Unpin>,
    ) -> Vec<Vec<u8>> {
        let mut chunks = Vec::new();
        let mut buf = [0u8; 512];
        loop {
            let n = tokio::io::AsyncReadExt::read(reader, &mut buf)
                .await
                .unwrap();
            if n == 0 {
                break;
            }
            chunks.push(buf[..n].to_vec());
        }
        assert_eq!(
            tokio::io::AsyncReadExt::read(reader, &mut buf)
                .await
                .unwrap(),
            0
        );
        chunks
    }

    smol::block_on(async {
        let mut futures_reader = ByteReader::new(msg_stream(msgs()));
        let futures_chunks = chunked_reads_futures(&mut futures_reader).await;

        let mut tokio_reader = ByteReader::new(msg_stream(msgs()));
        let tokio_chunks = chunked_reads_tokio(&mut tokio_reader).await;

        assert_eq!(
            futures_chunks, tokio_chunks,
            "futures-io and tokio entry points must agree on chunk boundaries"
        );
        let payload: Vec<u8> = futures_chunks.concat();
        let mut expected = b"hello ".to_vec();
        expected.extend_from_slice(&[0xCD; 10_000]);
        assert_eq!(payload, expected);
    });
}
