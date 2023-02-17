use async_trait::async_trait;
use enum_delegate::delegate;

use crate::errors;
use crate::request;

/// Entry point, and callback for a `Circuit` loop iteration.
///
/// Example implementation:
/// ```rust
/// #[delegate]
/// pub trait MyExt {
///     fn get_priority(&self) -> usize;
/// }
///
/// #[derive(Debug)]
/// #[delegate(derive(Task, MyExt))]
/// pub enum ExampleTaskEnum {
///     TaskOne(ExampleTaskOne),
///     // TaskTwo(ExampleTaskTwo),
/// }
///
/// #[derive(Debug)]
/// pub struct ExampleTaskOne {
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
/// impl MyExt for ExampleTaskOne {
///     fn get_priority(&self) -> usize {
///         self.priority
///     }
/// }
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
