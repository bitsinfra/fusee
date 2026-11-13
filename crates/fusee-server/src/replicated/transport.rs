//! The protocol thread can write to an idle peer without waking its writer.
//! A partially written frame stays with the socket that accepted its prefix.
use super::Traffic;
use crate::security::{Connector, Secured, TlsRead, TlsWrite};
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::Notify;

const QUEUE_CAPACITY: usize = 256;

/// A stream to a peer: plain TCP, or TLS whose write half has the same
/// non-blocking contract.
pub(super) enum PeerSocket {
    Plain(TcpStream),
    Tls(TlsWrite),
}

impl PeerSocket {
    fn try_write(&self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.try_write(bytes),
            Self::Tls(s) => s.try_write(bytes),
        }
    }

    async fn writable(&self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.writable().await,
            Self::Tls(s) => s.writable().await,
        }
    }
}

/// The peer never sends on this stream. Reading it lets TLS answer a key
/// update and notices the peer closing it.
async fn discard(mut inbound: TlsRead) {
    use tokio::io::AsyncReadExt;
    let mut buf = [0u8; 4096];
    while let Ok(1..) = inbound.read(&mut buf).await {}
}

struct Frame {
    bytes: Vec<u8>,
    offset: usize,
    queued: Option<Instant>,
}

struct Connection {
    socket: Arc<PeerSocket>,
    queue: VecDeque<Frame>,
    // Set throughout the asynchronous write, including while the frame is
    // outside the queue. New frames cannot overtake those bytes.
    writing: bool,
}

#[derive(Default)]
pub(super) struct Peer {
    connection: Mutex<Option<Connection>>,
    wake: Notify,
}

impl Peer {
    pub(super) fn send(&self, bytes: Vec<u8>, traffic: &Traffic) {
        let mut guard = self.connection.lock().unwrap();
        let Some(link) = guard.as_mut() else { return };
        let queued = traffic.timings.start();
        let mut offset = 0;
        if !link.writing && link.queue.is_empty() {
            match link.socket.try_write(&bytes) {
                Ok(n) if n > 0 => {
                    traffic.sent.fetch_add(n as u64, Ordering::Relaxed);
                    if n == bytes.len() {
                        traffic.timings.outbound_queue.observe(queued);
                        return;
                    }
                    offset = n;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                _ => {
                    *guard = None;
                    self.wake.notify_one();
                    return;
                }
            }
        }
        // On congestion drop a whole unsent frame. The protocol retransmits.
        // A direct partial write above always has an empty queue.
        if link.queue.len() < QUEUE_CAPACITY {
            link.queue.push_back(Frame {
                bytes,
                offset,
                queued,
            });
            self.wake.notify_one();
        }
    }

    /// This replica's stream to the peer is open: the peer process accepted
    /// it and no write has failed since.
    pub(super) fn connected(&self) -> bool {
        self.connection
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    pub(super) async fn run(&self, addr: &str, traffic: &Traffic, connector: &Connector) {
        loop {
            match TcpStream::connect(addr).await {
                Ok(socket) => {
                    let _ = socket.set_nodelay(true);
                    let (socket, inbound) = if connector.is_open() {
                        (PeerSocket::Plain(socket), None)
                    } else {
                        match connector.secure_tcp(socket, addr).await {
                            Ok(Secured::Plain(s)) => (PeerSocket::Plain(s), None),
                            Ok(Secured::Tls(s)) => {
                                let (read, write) = s.into_split();
                                (PeerSocket::Tls(write), Some(read))
                            }
                            Err(e) => {
                                crate::security::refused(
                                    format_args!("replica: could not secure the stream to {addr}"),
                                    &e,
                                );
                                // A refusal repeats until someone fixes the
                                // configuration: do not hammer the peer.
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }
                    };
                    let socket = Arc::new(socket);
                    *self.connection.lock().unwrap() = Some(Connection {
                        socket: socket.clone(),
                        queue: VecDeque::new(),
                        writing: false,
                    });
                    match inbound {
                        None => self.drain(&socket, traffic).await,
                        Some(inbound) => {
                            tokio::select! {
                                () = self.drain(&socket, traffic) => {}
                                () = discard(inbound) => {}
                            }
                        }
                    }
                    // Reconnection drops the entire old queue. A suffix can
                    // never become the first frame on a fresh stream.
                    *self.connection.lock().unwrap() = None;
                }
                // A member that just started is reached within 10 ms, well
                // inside an election timeout (frames to it are dropped until
                // then).
                Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    async fn drain(&self, socket: &PeerSocket, traffic: &Traffic) {
        loop {
            let batch = {
                let mut guard = self.connection.lock().unwrap();
                let Some(link) = guard.as_mut() else { return };
                link.writing = !link.queue.is_empty();
                link.queue.pop_front().map(|mut frame| {
                    traffic.timings.outbound_queue.observe(frame.queued);
                    while frame.bytes.len() < 1 << 20 {
                        let Some(more) = link.queue.pop_front() else {
                            break;
                        };
                        traffic.timings.outbound_queue.observe(more.queued);
                        frame.bytes.extend_from_slice(&more.bytes[more.offset..]);
                    }
                    frame
                })
            };
            let Some(mut frame) = batch else {
                self.wake.notified().await;
                continue;
            };
            while frame.offset < frame.bytes.len() {
                if socket.writable().await.is_err() {
                    return;
                }
                match socket.try_write(&frame.bytes[frame.offset..]) {
                    Ok(n) if n > 0 => {
                        traffic.sent.fetch_add(n as u64, Ordering::Relaxed);
                        frame.offset += n;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    _ => return,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::{TcpListener, TcpSocket};
    use tokio::time::timeout;

    #[tokio::test]
    async fn partial_frames_keep_their_place_while_the_peer_is_backpressured() {
        timeout(Duration::from_secs(20), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let client = TcpSocket::new_v4().unwrap();
            client.set_send_buffer_size(8192).unwrap();
            let socket = Arc::new(PeerSocket::Plain(
                client
                    .connect(listener.local_addr().unwrap())
                    .await
                    .unwrap(),
            ));
            let (mut reader, _) = listener.accept().await.unwrap();
            socket.writable().await.unwrap();
            let peer = Arc::new(Peer::default());
            let traffic = Arc::new(Traffic::default());
            *peer.connection.lock().unwrap() = Some(Connection {
                socket: socket.clone(),
                queue: VecDeque::new(),
                writing: false,
            });
            peer.send(b"header".to_vec(), &traffic);
            assert!(
                peer.connection
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .queue
                    .is_empty()
            );
            let body = vec![91; 4 << 20];
            peer.send(body.clone(), &traffic);
            {
                let g = peer.connection.lock().unwrap();
                let frame = g.as_ref().unwrap().queue.front().unwrap();
                assert!(frame.offset < body.len());
            }
            peer.send(b"after".to_vec(), &traffic);
            let drain = tokio::spawn({
                let peer = peer.clone();
                let traffic = traffic.clone();
                async move { peer.drain(&socket, &traffic).await }
            });
            let mut expected = b"header".to_vec();
            expected.extend(body);
            expected.extend_from_slice(b"after");
            let mut received = vec![0; expected.len()];
            reader.read_exact(&mut received).await.unwrap();
            assert_eq!(received, expected);
            assert_eq!(traffic.sent.load(Ordering::Relaxed), received.len() as u64);
            drain.abort();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn reconnect_discards_the_previous_streams_partial_frame() {
        timeout(Duration::from_secs(20), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let peer = Arc::new(Peer::default());
            let traffic = Arc::new(Traffic::default());
            let run = tokio::spawn({
                let peer = peer.clone();
                let traffic = traffic.clone();
                let open =
                    Connector::new(&Default::default(), crate::security::Purpose::Replica, 0)
                        .unwrap();
                async move { peer.run(&addr, &traffic, &open).await }
            });
            let (mut first, _) = listener.accept().await.unwrap();
            let before = loop {
                if let Some(socket) = peer
                    .connection
                    .lock()
                    .unwrap()
                    .as_ref()
                    .map(|l| l.socket.clone())
                {
                    break socket;
                }
                tokio::task::yield_now().await;
            };
            peer.send(vec![93; 16 << 20], &traffic);
            let mut prefix = [0; 32];
            first.read_exact(&mut prefix).await.unwrap();
            assert_eq!(prefix, [93; 32]);
            drop(first);
            let (mut next, _) = listener.accept().await.unwrap();
            loop {
                if peer
                    .connection
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|l| !Arc::ptr_eq(&l.socket, &before))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            peer.send(b"new frame".to_vec(), &traffic);
            let mut received = [0; 9];
            next.read_exact(&mut received).await.unwrap();
            assert_eq!(&received, b"new frame");
            run.abort();
        })
        .await
        .unwrap();
    }
}
