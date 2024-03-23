use anyhow::{Context, Error};
use byteorder::ReadBytesExt;
use pyo3::prelude::*;
use std::io::prelude::*;
use std::os::unix::net::UnixListener;

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub enum IterValue {
    Value(Vec<u8>),
    EndOfIter,
    Exception(String),
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
pub enum Message {
    Start(Vec<u8>),
}

pub fn write_bytes<W: Write>(w: &mut W, bytes: &[u8]) -> std::io::Result<()> {
    w.write_all(&(bytes.len() as u64).to_le_bytes())?;
    w.write_all(bytes)?;
    Ok(())
}

pub fn read_bytes<R: Read>(r: &mut R) -> std::io::Result<Vec<u8>> {
    let bytes_len = r.read_u64::<byteorder::LittleEndian>()?;
    let mut bytes = vec![0u8; bytes_len as usize];
    r.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn bail_on_pyerr<W: std::io::Write>(w: &mut W, msg: &str, err: PyErr, py: Python<'_>) -> Error {
    let err_trace = match err.traceback(py) {
        None => "no traceback".into(),
        Some(trace) => trace.format().unwrap_or_else(|_| "error collecting trace".to_string()),
    };
    let err = format!("{msg}: {err}\n{err_trace}");
    // These are hard failures to ensure that the process dies if it cannot communicates
    // with the other process.
    let item = match bincode::serialize(&IterValue::Exception(err)) {
        Ok(o) => o,
        Err(err) => return err.into(),
    };
    if let Err(err) = write_bytes(w, &item) {
        return err.into();
    }
    anyhow::format_err!("{msg}")
}

pub fn run_server(socket_file: &str, py: Python<'_>) -> anyhow::Result<()> {
    let unix_listener =
        UnixListener::bind(socket_file).context("unable to create the unix socket")?;

    let (mut w, _socket_address) = unix_listener.accept().context("accept error")?;
    let w = &mut w;
    let payload = read_bytes(w)?;
    let message: Message = bincode::deserialize(&payload)?;
    match message {
        Message::Start(bytes) => {
            let pickle_module = match py.import("dill") {
                Ok(m) => m,
                Err(err) => Err(bail_on_pyerr(w, "cannot import dill", err, py))?,
            };
            let bytes_obj = pyo3::types::PyBytes::new(py, &bytes);
            let f = match pickle_module.call_method1("loads", (bytes_obj,)) {
                Ok(m) => m,
                Err(err) => Err(bail_on_pyerr(w, "cannot call loads", err, py))?,
            };
            let iter = match f.call0() {
                Ok(m) => m,
                Err(err) => Err(bail_on_pyerr(w, "in closure", err, py))?,
            };
            loop {
                match iter.call_method0("__next__") {
                    Ok(res) => {
                        let res = pickle_module
                            .call_method1("dumps", (res,))
                            .and_then(|res| res.extract::<Vec<u8>>());
                        let res = match res {
                            Ok(m) => m,
                            Err(err) => Err(bail_on_pyerr(w, "dumps", err, py))?,
                        };
                        let res = bincode::serialize(&IterValue::Value(res))?;
                        write_bytes(w, &res)?;
                    }
                    Err(err) => {
                        if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                            let item = bincode::serialize(&IterValue::EndOfIter)?;
                            write_bytes(w, &item)?;
                        } else {
                            Err(bail_on_pyerr(w, "in __next__", err, py))?
                        }
                    }
                }
            }
        }
    }
}
