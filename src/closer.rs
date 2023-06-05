use std::sync::{Arc, Mutex};

use crossbeam_channel::{unbounded, Receiver, Sender};

type CloserChan = Arc<(Mutex<Option<Sender<()>>>, Receiver<()>)>;
type WaitChan = Arc<(Sender<()>, Receiver<()>)>;

// TODO: review closer implementation
#[derive(Clone)]
pub struct Closer {
    chan: CloserChan,
    wait: WaitChan,
}

impl Closer {
    pub fn new() -> Self {
        let (tx, rx) = unbounded::<()>();
        let (tx2, rx2) = unbounded::<()>();
        Self {
            chan: Arc::new((Mutex::new(Some(tx)), rx)),
            wait: Arc::new((tx2, rx2)),
        }
    }

    pub fn close(&self) {
        self.chan.0.lock().unwrap().take();
    }

    pub fn get_receiver(&self) -> &Receiver<()> {
        &self.chan.1
    }

    pub fn is_some(&self) -> bool {
        self.chan.0.lock().unwrap().is_some()
    }

    pub fn done(&self) {
        self.wait.0.send(()).unwrap();
    }

    pub fn wait_done(&self) {
        self.wait.1.recv().unwrap();
    }
}

#[cfg(test)]
mod tests {
    use yatp::task::callback::Handle;

    use super::*;

    #[test]
    fn test_closer() {
        let pool = yatp::Builder::new("test_closer").build_callback_pool();
        let closer = Closer::new();

        let (tx, rx) = unbounded::<()>();

        for _i in 0..10 {
            let closer = closer.clone();
            let tx = tx.clone();
            pool.spawn(move |_: &mut Handle<'_>| {
                assert!(closer.get_receiver().recv().is_err());
                tx.send(()).unwrap();
            });
        }

        closer.close();

        for _ in 0..10 {
            rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        }
    }
}
