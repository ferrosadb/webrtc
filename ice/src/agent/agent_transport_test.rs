use util::vnet::*;
use util::Conn;
use waitgroup::WaitGroup;

use super::agent_vnet_test::*;
use super::*;
use crate::agent::agent_transport::AgentConn;

pub(crate) async fn pipe(
    default_config0: Option<AgentConfig>,
    default_config1: Option<AgentConfig>,
) -> Result<(Arc<impl Conn>, Arc<impl Conn>, Arc<Agent>, Arc<Agent>)> {
    let (a_notifier, mut a_connected) = on_connected();
    let (b_notifier, mut b_connected) = on_connected();

    let mut cfg0 = default_config0.unwrap_or_default();
    cfg0.urls = vec![];
    cfg0.network_types = supported_network_types();

    let a_agent = Arc::new(Agent::new(cfg0).await?);
    a_agent.on_connection_state_change(a_notifier);

    let mut cfg1 = default_config1.unwrap_or_default();
    cfg1.urls = vec![];
    cfg1.network_types = supported_network_types();

    let b_agent = Arc::new(Agent::new(cfg1).await?);
    b_agent.on_connection_state_change(b_notifier);

    let (a_conn, b_conn) = connect_with_vnet(&a_agent, &b_agent).await?;

    // Ensure pair selected
    // Note: this assumes ConnectionStateConnected is thrown after selecting the final pair
    let _ = a_connected.recv().await;
    let _ = b_connected.recv().await;

    Ok((a_conn, b_conn, a_agent, b_agent))
}

#[tokio::test]
async fn test_remote_local_addr() -> Result<()> {
    // Agent0 is behind 1:1 NAT
    let nat_type0 = nat::NatType {
        mode: nat::NatMode::Nat1To1,
        ..Default::default()
    };
    // Agent1 is behind 1:1 NAT
    let nat_type1 = nat::NatType {
        mode: nat::NatMode::Nat1To1,
        ..Default::default()
    };

    let v = build_vnet(nat_type0, nat_type1).await?;

    let stun_server_url = Url {
        scheme: SchemeType::Stun,
        host: VNET_STUN_SERVER_IP.to_owned(),
        port: VNET_STUN_SERVER_PORT,
        proto: ProtoType::Udp,
        ..Default::default()
    };

    //"Disconnected Returns nil"
    {
        let disconnected_conn = AgentConn::new();
        let result = disconnected_conn.local_addr();
        assert!(result.is_err(), "Disconnected Returns nil");
    }

    //"Remote/Local Pair Match between Agents"
    {
        let (ca, cb) = pipe_with_vnet(
            &v,
            AgentTestConfig {
                urls: vec![stun_server_url.clone()],
                ..Default::default()
            },
            AgentTestConfig {
                urls: vec![stun_server_url],
                ..Default::default()
            },
        )
        .await?;

        let a_laddr = ca.local_addr()?;
        let b_laddr = cb.local_addr()?;

        // Assert addresses
        assert_eq!(a_laddr.ip().to_string(), VNET_LOCAL_IPA.to_string());
        assert_eq!(b_laddr.ip().to_string(), VNET_LOCAL_IPB.to_string());

        // Close
        //ca.close().await?;
        //cb.close().await?;
    }

    v.close().await?;

    Ok(())
}

#[tokio::test]
async fn test_conn_stats() -> Result<()> {
    let (ca, cb, _, _) = pipe(None, None).await?;
    let na = ca.send(&[0u8; 10]).await?;

    let wg = WaitGroup::new();

    let w = wg.worker();
    tokio::spawn(async move {
        let _d = w;

        let mut buf = vec![0u8; 10];
        let nb = cb.recv(&mut buf).await?;
        assert_eq!(nb, 10, "bytes received don't match");

        Result::<()>::Ok(())
    });

    wg.wait().await;

    assert_eq!(na, 10, "bytes sent don't match");

    Ok(())
}

/// Descriptors this process holds open, via `/dev/fd` (macOS and Linux).
#[cfg(unix)]
fn open_descriptors() -> usize {
    std::fs::read_dir("/dev/fd").expect("/dev/fd").count()
}

/// After `Agent::close`, none of the agent's candidate sockets may survive —
/// even while the `Conn` the agent handed out is still held by the caller.
///
/// Every host candidate owns a UDP socket. `close` deletes the agent's
/// candidate lists, but the connectivity checklist and selected pair live on
/// the `AgentConn` handed to the caller, and each pair holds its local
/// candidate, which holds the socket. An upper layer that keeps the conn a
/// moment longer (a reader task draining, a mux closing) therefore keeps every
/// socket the agent ever gathered — and a layer that keeps it forever leaks
/// them all. Measured downstream at one socket per candidate per session,
/// until the process hit its descriptor limit and could gather nothing.
///
/// Holding the conns across the close here is the point of the test.
#[cfg(unix)]
#[tokio::test]
async fn test_close_releases_candidate_sockets_while_conn_is_held() -> Result<()> {
    let baseline = open_descriptors();
    let (ca, cb, a_agent, b_agent) = pipe(None, None).await?;
    let during = open_descriptors();
    assert!(
        during > baseline,
        "a connected pair of agents holds sockets: baseline={baseline} during={during}"
    );

    a_agent.close().await?;
    b_agent.close().await?;

    // Socket release rides on task teardown, which is asynchronous; wait a
    // bounded moment rather than asserting on the first read.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut after = open_descriptors();
    while after > baseline + 2 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        after = open_descriptors();
    }
    assert!(
        after <= baseline + 2,
        "closed agents must release their candidate sockets even while their conns are held: \
         baseline={baseline} during={during} after={after}"
    );

    drop(ca);
    drop(cb);
    Ok(())
}

/// A listener serves session after session in one process. The second must
/// connect as readily as the first.
///
/// This is the ICE-layer statement of the regression that kept the socket
/// release fix out of ferrosa-memory: with that fix patched in, the SECOND
/// full control session in a process never opened its data channel, while
/// stock 0.17.1 and 0.17.2 both managed it. If the cause is in this crate,
/// this test is where it shows: two `pipe()`s back to back, each proving it
/// carries a datagram, with the first pair closed before the second begins.
///
/// Timed rather than left to hang — a session that never connects is the
/// failure under test, and a test that hangs reports nothing.
#[cfg(unix)]
#[tokio::test]
async fn a_second_session_connects_after_the_first_one_closed() -> Result<()> {
    const SESSIONS: usize = 2;
    const PER_SESSION: Duration = Duration::from_secs(30);

    for session in 0..SESSIONS {
        let connected = tokio::time::timeout(PER_SESSION, pipe(None, None))
            .await
            .unwrap_or_else(|_| {
                panic!("session {session} did not connect within {PER_SESSION:?}")
            })?;
        let (ca, cb, a_agent, b_agent) = connected;

        // Connected is not the same as usable, and the downstream symptom was
        // a connection that formed and then carried nothing.
        ca.send(b"ping").await?;
        let mut buf = vec![0_u8; 16];
        let n = tokio::time::timeout(PER_SESSION, cb.recv(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("session {session} carried no datagram"))?;
        assert_eq!(&buf[..n], b"ping", "session {session} payload");

        a_agent.close().await?;
        b_agent.close().await?;
        drop(ca);
        drop(cb);
    }
    Ok(())
}
