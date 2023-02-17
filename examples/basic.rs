use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use concurrent_tor::delegate;
use concurrent_tor::anyhow;
use concurrent_tor::hyper;
use concurrent_tor::tokio;
use concurrent_tor::async_trait;

use concurrent_tor::log;
use concurrent_tor::logging;

use concurrent_tor;
use concurrent_tor::{arc, std_mutex};
use concurrent_tor::circuit_handler;
use concurrent_tor::configs;
use concurrent_tor::errors;
use concurrent_tor::request;

use concurrent_tor::dispatcher::Dispatcher;
use concurrent_tor::task::Task;

#[delegate]
pub trait MyExt {
    fn get_priority(&self) -> usize;
}

#[derive(Debug)]
#[delegate(derive(Task, MyExt))]
pub enum ExampleTaskEnum {
    TaskOne(ExampleTaskOne),
    // TaskTwo(ExampleTaskTwo),
}

#[derive(Debug)]
pub struct ExampleTaskOne {
    priority: usize,
    task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>>,
    request: request::Request,
}

impl ExampleTaskOne {
    fn new(priority: usize, task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>>, request: request::Request)
           -> ExampleTaskOne {
        ExampleTaskOne { priority, task_dispatcher, request }
    }
}

#[async_trait]
impl Task for ExampleTaskOne {
    // The request can either be generated on each call of this function, or it can be stored in the
    // struct. The advantage of storing in the struct is that, if a retry is necessary, the original
    // Task object can be reused. Storing the request in the struct means errors (i.e. uri parsing)
    // can be handled elsewhere.
    fn get_request(&mut self) -> &mut request::Request {
        &mut self.request
    }

    async fn request_completed(
        &mut self,
        response: errors::RequestResult,
    ) -> Result<(), anyhow::Error> {
        println!("{:?}", self.request.get_next_attempt());
        println!("{:?}", response?.body_bytes().await?);
        // logic to decide if the task was successful (i.e. insert to a database)

        // or if it failed due to a network error (i.e. check self.can_try() == true, if no, maybe
        // you want to increase max_tries, self.next_attempt = request::RequestResult::Retry)

        // or if the page is no longer online (i.e. self.can_try() == true ... ,
        // self.next_attempt = request::RequestResult::AttemptWebArchive)
        self.request.next_attempt(request::RequestType::WebArchive);
        Ok(())
    }
}

impl MyExt for ExampleTaskOne {
    fn get_priority(&self) -> usize {
        self.priority
    }
}

#[derive(Debug)]
pub struct ExampleTaskDispatcher<T> {
    tasks: Arc<Mutex<Vec<T>>>,
}

impl<T> ExampleTaskDispatcher<T>
    where
        T: Task + MyExt,
{
    fn new() -> ExampleTaskDispatcher<T> {
        let tasks = arc!(std_mutex!(vec![]));
        ExampleTaskDispatcher { tasks }
    }
}

impl<T> Dispatcher<T> for ExampleTaskDispatcher<T>
    where
        T: Task + MyExt,
{
    fn get_task(&self) -> Option<T> {
        let mut guard = self.tasks.lock().unwrap();
        let rtn_value = guard.pop();
        rtn_value
    }

    fn add_task(&self, task: T) {
        let mut guard = self.tasks.lock().unwrap();
        dbg!(task.get_priority());
        guard.push(task);
    }
}

async fn my_long_running_task(task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>>) {
    for i in 0..1 {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(hyper::header::USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36".parse().unwrap());
        // Test headers: https://www.httpbin.org/headers
        // Test public ip: https://www.httpbin.org/ip
        let request = request::Request::default()
            .uri("https://www.wikipedia.org/".parse::<hyper::Uri>().unwrap())
            .max_tries(3)
            .allow_redirect(true)
            .headers(headers);
        let task = ExampleTaskOne::new(i, task_dispatcher.clone(), request);

        task_dispatcher.add_task(ExampleTaskEnum::TaskOne(task));
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    tokio::time::sleep(Duration::from_secs(1000)).await;
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let log_level = log::LevelFilter::Debug;
    let basic_logger = logging::BasicLogger::new(log_level);
    logging::init_logging(basic_logger, log_level);

    let task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>> =
        arc!(ExampleTaskDispatcher::new());

    let dispatcher_config = configs::CircuitHandlerConfig::new(2, 0.0, 2.0);
    let mut circuit_handler = circuit_handler::CircuitHandler::new(dispatcher_config);

    let _ = circuit_handler
        .build_circuits(task_dispatcher.clone())
        .await?;

    let handle = tokio::spawn({
        async move {
            circuit_handler.run().await.unwrap();
        }
    });

    my_long_running_task(task_dispatcher).await;

    handle.abort();

    Ok(())
}
