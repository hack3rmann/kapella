use display_error_chain::ErrorChainExt as _;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let main_cancel = CancellationToken::new();

    let cancel = main_cancel.clone();
    let mut main = tokio::spawn(async move {
        loop {
            select! {
                _ = task::yield_now() => {}
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }

        Ok::<(), ()>(())
    });

    let mut enable_stop_signal = true;

    loop {
        select! {
            result = &mut main => {
                if let Err(error) = result {
                    error!(error = %error.chain(), "main task failed");
                }

                info!("the daemon has finished");

                break;
            }
            result = signal::ctrl_c(), if enable_stop_signal => {
                if let Err(error) = result {
                    error!(error = %error.chain(), "failed to wait for Ctrl+C");
                }

                info!("got stop signal, shutting down the daemon");

                main_cancel.cancel();
                enable_stop_signal = false;
            }
        }
    }
}
