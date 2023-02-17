use async_trait::async_trait;
use enum_delegate::delegate;

use crate::errors;
use crate::request;

/// Entry point, and callback for a `Circuit` loop iteration.
///
/// Example implementation:
/// ```rust
/// use std::sync::Arc;
/// use std::sync::Mutex;
///
/// use concurrent_tor::request;
/// use concurrent_tor::errors;
/// use concurrent_tor::delegate;
/// use concurrent_tor::async_trait;
/// use concurrent_tor::{arc, std_mutex};
/// use concurrent_tor::task::Task;
/// use concurrent_tor::dispatcher::Dispatcher;
///
/// # #[derive(Debug)]
/// # struct ExampleTaskDispatcher<T> {
/// #     tasks: Arc<Mutex<Vec<T>>>,
/// # }
/// # impl<T> ExampleTaskDispatcher<T>
/// #     where
/// #         T: Task,  // + MyExt
/// # {
/// #     fn new() -> ExampleTaskDispatcher<T> {
/// #         let tasks = arc!(std_mutex!(vec![]));
/// #         ExampleTaskDispatcher { tasks }
/// #     }
/// # }
/// # impl<T> Dispatcher<T> for ExampleTaskDispatcher<T>
/// #     where
/// #         T: Task,  // + MyExt
/// # {
/// #     fn get_task(&self) -> Option<T> {
/// #         let mut guard = self.tasks.lock().unwrap();
/// #         let rtn_value = guard.pop();
/// #         rtn_value
/// #     }
/// #     fn add_task(&self, task: T) {
/// #         let mut guard = self.tasks.lock().unwrap();
/// #         // dbg!(task.get_priority());
/// #         guard.push(task);
/// #     }
/// # }
/// // Add the following to `Cargo.toml` if specifying your own trait:
/// // enum_delegate = { git = "https://gitlab.com/Sean-McConnachie/enum_delegate_0.3.0" }
///
/// // #[delegate]
/// // trait MyExt {
/// //     fn get_priority(&self) -> usize;
/// // }
///
/// #[derive(Debug)]
/// #[delegate(derive(Task))]  // ...derive(Task, MyExt)...
/// enum ExampleTaskEnum {
///     TaskOne(ExampleTaskOne),
///     // TaskTwo(ExampleTaskTwo),
/// }
///
/// #[derive(Debug)]
/// struct ExampleTaskOne {
///     priority: usize,
///     task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>>,
///     request: request::Request,
/// }
///
/// impl ExampleTaskOne {
///     fn new(priority: usize, task_dispatcher: Arc<ExampleTaskDispatcher<ExampleTaskEnum>>, request: request::Request)
///            -> ExampleTaskOne {
///         ExampleTaskOne { priority, task_dispatcher, request }
///     }
/// }
///
/// #[async_trait]
/// impl Task for ExampleTaskOne {
///     // The request can either be generated on each call of this function, or it can be stored in the
///     // struct. The advantage of storing in the struct is that, if a retry is necessary, the original
///     // Task object can be reused. Storing the request in the struct means errors (i.e. uri parsing)
///     // can be handled elsewhere.
///     fn get_request(&mut self) -> &mut request::Request {
///         &mut self.request
///     }
///
///     async fn request_completed(
///         &mut self,
///         response: errors::RequestResult,
///     ) -> Result<(), anyhow::Error> {
///         println!("{:?}", self.request.get_next_attempt());
///         println!("{:?}", response?.body_bytes().await?);
///         // logic to decide if the task was successful (i.e. insert to a database)
///
///         // or if it failed due to a network error (i.e. check self.can_try() == true, if no, maybe
///         // you want to increase max_tries, self.next_attempt = request::RequestResult::Retry)
///
///         // or if the page is no longer online (i.e. self.can_try() == true ... ,
///         // self.next_attempt = request::RequestResult::AttemptWebArchive)
///         self.request.next_attempt(request::RequestType::WebArchive);
///         Ok(())
///     }
/// }
///
/// // impl MyExt for ExampleTaskOne {
/// //     fn get_priority(&self) -> usize {
/// //         self.priority
/// //     }
/// // }
/// ```

#[async_trait]
#[delegate]
pub trait Task {
    fn get_request(&mut self) -> &mut request::Request;
    async fn request_completed(
        &mut self,
        response: errors::RequestResult,
    ) -> Result<(), anyhow::Error>;
}
