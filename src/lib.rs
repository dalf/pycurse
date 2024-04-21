use pyo3::prelude::*;
use std::collections::HashMap;
use std::str;
use std::thread;
use std::time::Duration;
use curl::easy::{Easy2, Handler, WriteError};
use curl::multi::{Easy2Handle, Multi};
use std::result::Result;
use std::sync::Mutex;
use crossbeam::channel::{unbounded, Sender, Receiver};
use lazy_static::lazy_static;


struct Response {
    url: String,
    status_code: i64,
    data: Vec<u8>,
}


struct Collector(Vec<u8>);
impl Handler for Collector {
    fn write(&mut self, data: &[u8]) -> Result<usize, WriteError> {
        self.0.extend_from_slice(data);
        Ok(data.len())
    }
}

struct Downloader {
    task_sender: Sender<String>,
    task_receiver: Receiver<String>,
    response_sender: Sender<Response>,
    response_receiver: Receiver<Response>,
    running: bool,
}

impl Drop for Downloader {
    fn drop(&mut self) {
        self.running = false;
    }
}

impl Downloader {
    fn new() -> Self {
        let (task_sender, task_receiver) =  unbounded();
        let (response_sender, response_receiver) = unbounded();

        Downloader {
            task_sender: task_sender,
            task_receiver: task_receiver,
            response_sender: response_sender,
            response_receiver: response_receiver,
            running: true,
        }
    }

    fn add_request(&mut self, url: &str) -> PyResult<()> {
        match self.task_sender.send(url.to_owned()) {
            Err(_) => return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>("Failed to send task")),
            Ok(_) => return Ok(()),
        }
    }

    fn get_response(&mut self) -> Result<Response, std::sync::mpsc::RecvError> {
        match self.response_receiver.recv() {
            Ok(response) => Ok(response),
            Err(_) => Err(std::sync::mpsc::RecvError),
        }
    }

    fn get_task(&mut self, processing_requests: bool) -> Result<String, std::sync::mpsc::RecvError> {
        if !processing_requests {
            // block if there is no download
            match self.task_receiver.recv_timeout(Duration::from_millis(500)) {
                Ok(url) => return Ok(url),
                Err(_) => return Err(std::sync::mpsc::RecvError),
            }
        }
        match self.task_receiver.try_recv() {
            Ok(url) => Ok(url),
            Err(_) => Err(std::sync::mpsc::RecvError),
        }
    }

    fn thread_runner(&mut self) {
        let multi = Multi::new();
        let mut handles: HashMap<usize, Easy2Handle<Collector>> = HashMap::new();
        let mut urls: HashMap<usize, String> = HashMap::new();
        let mut last_token = 0;
    
        let mut processing_requests = true;
        while self.running {
            println!("loop");

            match self.get_task(processing_requests) {
                Ok(url) => {
                    processing_requests = true;
                    println!("Add request");

                    let token = last_token;
                    last_token += 1;
            
                    //
                    let version = curl::Version::get();
                    let mut request = Easy2::new(Collector(Vec::new()));
                    request.url(&url).unwrap();
                    request.useragent(&format!("curl/{}", version.version())).unwrap();
                
                    let mut handle = multi.add2(request).unwrap();
                    handle.set_token(token).unwrap();
            
                    //
                    handles.insert(token, handle);
                    urls.insert(token, url.to_owned());
                }
                Err(_) => {
                    // No more tasks to process.
                }
            }

            // We still need to process the last messages when
            // `Multi::perform` returns "0".
            if multi.perform().unwrap() == 0 {
                processing_requests = false;
                println!("No more");
            }
    
            multi.messages(|message| {
                let token = message.token().expect("failed to get the token");
                let handle = handles
                    .get_mut(&token)
                    .expect("the download value should exist in the HashMap");
    
                match message
                    .result_for2(&handle)
                    .expect("token mismatch with the `EasyHandle`")
                {
                    Ok(()) => {
                        let http_status = handle
                            .response_code()
                            .expect("HTTP request finished without status code");

                        println!("Response!!");
                        self.response_sender.send(Response {
                            url: urls[&token].clone(),
                            status_code: http_status as i64,
                            data: handle.get_ref().0.clone(),
                        }).unwrap();
                    }
                    Err(error) => {
                        println!("Error!! {}", error);
                        self.response_sender.send(Response {
                            url: urls[&token].clone(),
                            status_code: -1,
                            data: Vec::new(),
                        }).unwrap();
                    }
                }
            });
    
            if processing_requests {
                // The sleeping time could be reduced to allow other processing.
                // For instance, a thread could check a condition signalling the
                // thread shutdown.
                multi.wait(&mut [], Duration::from_millis(10)).unwrap();
            }
        }
    }
}

lazy_static! {
    static ref DOWNLOADER: Mutex<Downloader> = Mutex::new(Downloader::new());
}


#[pyclass]
struct ResponsePython {
    url: String,
    status_code: i64,
    data: String,
}

#[pymethods]
impl ResponsePython {
    #[getter]
    fn url(&self) -> &str {
        &self.url
    }

    #[getter]
    fn status_code(&self) -> i64 {
        self.status_code
    }

    #[getter]
    fn data(&self) -> &str {
        &self.data
    }
}

/// A struct to store a curl easy handle.
#[pyclass]
struct CurlDownloader {
}

#[pymethods]
impl CurlDownloader {
    #[new]
    fn new() -> Self {
        CurlDownloader {
        }
    }

    /// Initialize curl downloader with the URL.
    fn add_request(&mut self, url: &str) -> PyResult<()> {
        let mut downloader = DOWNLOADER.lock().unwrap();
        downloader.add_request(url).unwrap();
        return Ok(());
    }

    /// Start download and read data by chunks.
    fn fetch(&mut self, timeout: u64) -> PyResult<Option<ResponsePython>> {
        println!("fetch");
        let downloader = DOWNLOADER.lock().unwrap();
        println!("fetch - downloader locked");
        let receiver = downloader.response_receiver.clone();
        match receiver.recv_timeout(Duration::from_millis(timeout)) {
            Ok(response) => {
                println!("fetch - response");
                return Ok(Some(ResponsePython {
                    url: response.url,
                    status_code: response.status_code,
                    data: str::from_utf8(&response.data).unwrap().to_owned(),
                }));
            }
            Err(_) => {
                println!("fetch - error");
                return Ok(None);
            }
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn pycurse(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CurlDownloader>()?;

    // start downloader thread
    thread::spawn(move || {
        let mut downloader = DOWNLOADER.lock().unwrap();
        downloader.thread_runner();
    });

    //
    Ok(())
}
