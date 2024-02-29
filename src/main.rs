use axum::body::Bytes;
use axum::http::header::UPGRADE;
use axum::http::{HeaderValue, Request, Response, StatusCode};
use http_body_util::Empty;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::sync::watch::Receiver;

// A simple type alias so as to DRY.
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Handle server-side I/O after HTTP upgraded.
async fn server_upgraded_io(upgraded: Upgraded) -> Result<()> {
    // downcast to the underlying TcpStream
    let parts = upgraded
        .downcast::<TokioIo<TcpStream>>()
        .map_err(|_| "server: Unable to downcast Upgraded")?;

    let mut conn = parts.io.into_inner();

    println!("server: successfully downcast to the underlying TcpStream");

    let mut vec = vec![0; 7];
    conn.read_exact(&mut vec).await?;
    println!("server[foobar] recv: {:?}", std::str::from_utf8(&vec));
    conn.write_all(b"bar=foo").await?;
    println!("server[foobar] sent");
    Ok(())
}

/// Our server HTTP handler to initiate HTTP upgrades.
async fn server_upgrade(mut req: Request<hyper::body::Incoming>) -> Result<Response<Empty<Bytes>>> {
    let mut res = Response::new(Empty::new());

    // Send a 400 to any request that doesn't have
    // an `Upgrade` header.
    if !req.headers().contains_key(UPGRADE) {
        *res.status_mut() = StatusCode::BAD_REQUEST;
        return Ok(res);
    }

    // Setup a future that will eventually receive the upgraded
    // connection and talk a new protocol, and spawn the future
    // into the runtime.
    //
    // Note: This can't possibly be fulfilled until the 101 response
    // is returned below, so it's better to spawn this future instead
    // waiting for it to complete to then return a response.
    tokio::task::spawn(async move {
        match hyper::upgrade::on(&mut req).await {
            Ok(upgraded) => {
                if let Err(e) = server_upgraded_io(upgraded).await {
                    eprintln!("server foobar io error: {}", e)
                };
            }
            Err(e) => eprintln!("upgrade error: {}", e),
        }
    });

    // Now return a 101 Response saying we agree to the upgrade to some
    // made-up 'foobar' protocol.
    *res.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
    res.headers_mut()
        .insert(UPGRADE, HeaderValue::from_static("foobar"));
    Ok(res)
}

/// Handle client-side I/O after HTTP upgraded.
async fn client_upgraded_io(upgraded: Upgraded) -> Result<()> {
    let mut upgraded = TokioIo::new(upgraded);
    // We've gotten an upgraded connection that we can read
    // and write directly on. Let's start out 'foobar' protocol.
    upgraded.write_all(b"foo=bar").await?;
    println!("client[foobar] sent");

    let mut vec = Vec::new();
    upgraded.read_to_end(&mut vec).await?;
    println!("client[foobar] recv: {:?}", std::str::from_utf8(&vec));

    Ok(())
}

/// Our client HTTP handler to initiate HTTP upgrades.
async fn client_upgrade_request(addr: SocketAddr) -> Result<()> {
    let req = Request::builder()
        .uri(format!("http://{}/", addr))
        .header(UPGRADE, "foobar")
        .body(Empty::<Bytes>::new())
        .unwrap();

    let stream = TcpStream::connect(addr).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

    tokio::task::spawn(async move {
        // Don't forget to enable upgrades on the connection.
        if let Err(err) = conn.with_upgrades().await {
            println!("Connection failed: {:?}", err);
        }
    });

    let res = sender.send_request(req).await?;

    if res.status() != StatusCode::SWITCHING_PROTOCOLS {
        panic!("Our server didn't upgrade: {}", res.status());
    }

    match hyper::upgrade::on(res).await {
        Ok(upgraded) => {
            if let Err(e) = client_upgraded_io(upgraded).await {
                eprintln!("client foobar io error: {}", e)
            };
        }
        Err(e) => eprintln!("upgrade error: {}", e),
    }

    Ok(())
}

fn broken_server_upgrade(listener: TcpListener, mut rx: Receiver<bool>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, _) = res.expect("Failed to accept");
                    let io = TokioIo::new(stream);

                    tokio::task::spawn(async move {
                        let conn = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(io, service_fn(server_upgrade)).await;
                        if let Err(err) = conn {
                            eprintln!("Error serving connection: {:?}", err);
                        }
                    });
                }
                _ = rx.changed() => {
                    break;
                }
            }
        }
    });
}

fn working_server_upgrade(listener: TcpListener, mut rx: Receiver<bool>) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                res = listener.accept() => {
                    let (stream, _) = res.expect("Failed to accept");
                    let io = TokioIo::new(stream);

                    let mut rx = rx.clone();
                    tokio::task::spawn(async move {
                        let conn = http1::Builder::new().serve_connection(io, service_fn(server_upgrade));

                        // Don't forget to enable upgrades on the connection.
                        let mut conn = conn.with_upgrades();

                        let mut conn = Pin::new(&mut conn);

                        tokio::select! {
                            res = &mut conn => {
                                if let Err(err) = res {
                                    println!("Error serving connection: {:?}", err);
                                }
                            }
                            // Continue polling the connection after enabling graceful shutdown.
                            _ = rx.changed() => {
                                conn.graceful_shutdown();
                            }
                        }
                    });
                }
                _ = rx.changed() => {
                    break;
                }
            }
        }
    });
}

async fn run(server_fn: fn(TcpListener, Receiver<bool>)) {
    // For this example, we just make a server and our own client to talk to
    // it, so the exact port isn't important. Instead, let the OS give us an
    // unused port.
    let addr: SocketAddr = ([127, 0, 0, 1], 0).into();

    let listener = TcpListener::bind(addr).await.expect("failed to bind");

    // We need the assigned address for the client to send it messages.
    let addr = listener.local_addr().unwrap();

    // For this example, a oneshot is used to signal that after 1 request,
    // the server should be shutdown.
    let (tx, rx) = watch::channel(false);

    // Spawn server on the default executor,
    // which is usually a thread-pool from tokio default runtime.
    server_fn(listener, rx);

    // Client requests a HTTP connection upgrade.
    let request = client_upgrade_request(addr);
    if let Err(e) = request.await {
        eprintln!("client error: {}", e);
    }

    // Complete the oneshot so that the server stops
    // listening and the process can close down.
    let _ = tx.send(true);
}

#[tokio::main]
async fn main() {
    println!("Running working server upgrade");
    run(working_server_upgrade).await;
    println!("Running broken server upgrade");
    run(broken_server_upgrade).await;
}
