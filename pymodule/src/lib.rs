use pyo3::prelude::*;
use std::os::unix::net::UnixStream;
use std::process::Command;
use std::sync::{Arc, Mutex};

use hojo::{IterValue, Message};

const DEBUG: bool = false;

fn w<E: ToString>(err: E) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(err.to_string())
}

#[macro_export]
macro_rules! py_bail {
    ($msg:literal $(,)?) => {
        return Err(pyo3::exceptions::PyValueError::new_err(format!($msg)))
    };
    ($err:expr $(,)?) => {
        return Err(pyo3::exceptions::PyValueError::new_err(format!($err)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err(pyo3::exceptions::PyValueError::new_err(format!($fmt, $($arg)*)))
    };
}

#[pyclass]
#[derive(Clone, Debug)]
struct Worker {
    // It's unclear whether the Arc is needed or whether it would
    // be possible to extract a ref from the Py<Worker> object.
    pid: u32,
    child: Arc<Mutex<(std::process::Child, UnixStream)>>,
    socket_file: String,
    rx: Arc<Mutex<std::sync::mpsc::Receiver<IterValue>>>,
}

impl Drop for Worker {
    fn drop(&mut self) {
        if DEBUG {
            eprintln!(
                "dropping Worker pid: {}, weak cnt: {} strong cnt: {}",
                self.pid,
                Arc::weak_count(&self.child),
                Arc::strong_count(&self.child),
            );
        }
        if let Ok(child) = &mut self.child.lock() {
            if let Err(err) = child.0.kill() {
                eprintln!("error killing worker {}: {}", self.pid, err)
            }
        }
        if let Err(err) = std::fs::remove_file(&self.socket_file) {
            eprintln!("IO error removing socket file for worker {}: {}", self.pid, err)
        }
    }
}

#[pymethods]
impl Worker {
    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn status(&self) -> PyResult<String> {
        if let Ok(child) = &mut self.child.lock() {
            let peer_addr = child.1.peer_addr()?;
            let so_error = match child.1.take_error()? {
                None => "ok".to_string(),
                Some(err) => format!("err: {err}"),
            };
            let s = format!("addr: {peer_addr:?}, {so_error}");
            Ok(s)
        } else {
            Ok("mutex-err".to_string())
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRef<'_, Self>) -> PyResult<Option<PyObject>> {
        let py = slf.py();
        let check_signals_every = std::time::Duration::from_millis(500);
        loop {
            let rx = &slf.rx;
            let value_or_timeout = py.allow_threads(move || {
                let rx = rx.lock().unwrap();
                rx.recv_timeout(check_signals_every)
            });
            match value_or_timeout {
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(None),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => py.check_signals()?,
                Ok(iter_value) => match iter_value {
                    IterValue::Exception(exn) => {
                        py_bail!("error in worker: {exn}")
                    }
                    IterValue::EndOfIter => return Ok(None),
                    IterValue::Value(bytes) => {
                        let py = slf.py();
                        let pickle_module = py.import("dill")?;
                        let bytes = pyo3::types::PyBytes::new(py, &bytes);
                        let obj = pickle_module.call_method1("loads", (bytes,))?;
                        return Ok(Some(obj.into_py(py)));
                    }
                },
            }
        }
    }
}

#[pyfunction]
fn run_server(socket_file: &str, py: Python) -> PyResult<()> {
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
    }
    hojo::run_server(socket_file, py).map_err(w)
}

#[pyfunction]
#[pyo3(signature = (f, *, binary=None))]
fn run_in_worker(f: PyObject, binary: Option<&str>, py: Python) -> PyResult<Py<Worker>> {
    if !f.as_ref(py).is_callable() {
        py_bail!("f argument is not callable")
    }

    let socket_file = "/tmp/plugin-sock";
    let _rm = std::fs::remove_file(socket_file);
    let mut child = match binary {
        Some(binary) => Command::new(binary).arg("--socket-file").arg(socket_file).spawn(),
        None => {
            let sys = py.import("sys")?;
            let binary: String = sys.getattr("executable")?.extract()?;
            let server_py = format!("import hojo; hojo.run_server('{socket_file}')");
            Command::new(binary).arg("-c").arg(server_py).spawn()
        }
    }
    .map_err(w)?;
    let pid = child.id();
    if DEBUG {
        println!("spawned child with pid {pid}");
    }

    let mut duration = std::time::Duration::from_millis(10);
    let mut stream = 'stream: {
        for _ in 0..5 {
            std::thread::sleep(duration);
            if let Ok(stream) = UnixStream::connect(socket_file) {
                break 'stream stream;
            }
            duration *= 2;
        }
        // Time-out, let's try to clean things up.
        if let Err(err) = child.kill() {
            eprintln!("error killing worker {}: {}", pid, err)
        }
        if let Err(err) = std::fs::remove_file(socket_file) {
            eprintln!("IO error removing socket file for worker {}: {}", pid, err)
        }
        py_bail!("timed out connecting to worker {}", pid);
    };

    let pickle_module = py.import("dill")?;
    let f = pickle_module.call_method1("dumps", (f,))?;
    let f = f.extract::<Vec<u8>>()?;
    let message = bincode::serialize(&Message::Start(f)).map_err(w)?;
    hojo::write_bytes(&mut stream, &message)?;
    let (tx, rx) = std::sync::mpsc::sync_channel(16);
    let mut stream_t = stream.try_clone()?;
    std::thread::spawn(move || loop {
        let payload = match hojo::read_bytes(&mut stream_t) {
            Ok(l) => l,
            Err(err) => {
                eprintln!("error reading from worker stream {err:?}");
                break;
            }
        };
        let payload: IterValue = match bincode::deserialize(&payload) {
            Ok(l) => l,
            Err(err) => {
                eprintln!("error deserializing message from worker stream {err:?}");
                break;
            }
        };
        let eos = payload == IterValue::EndOfIter;
        if tx.send(payload).is_err() {
            break;
        }
        if eos {
            break;
        }
    });

    let worker = Worker {
        pid,
        child: Arc::new(Mutex::new((child, stream))),
        socket_file: socket_file.to_owned(),
        rx: Arc::new(Mutex::new(rx)),
    };

    Python::with_gil(|py| {
        let obj = Py::new(py, worker)?;
        Ok(obj)
    })
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "hojo")]
fn hojo_(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Worker>()?;
    m.add_function(wrap_pyfunction!(run_in_worker, m)?)?;
    m.add_function(wrap_pyfunction!(run_server, m)?)?;
    Ok(())
}
