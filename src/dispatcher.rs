use crate::task;

// TODO: Have Dispatcher implement Deref and DerefMut. Allow user to interat with it as a standard vec.
// I.e. pub trait Dispatcher: Deref + DerefMut { ... }
// default_deref_mut_impls!(UserDispatcher)
// impls of Deref and DerefMut return MutexGuards. User can define their own .pop/.extend/.push functions.

/// User defined dispatccher which returns structs that implement the `Task` trait.
///
/// Define a queue/binary heap here. In general, the user-defined `Dispatcher` should have a field such
/// as `Arc<Mutex<Vec<T>>>` where `T: Task`.
///
/// Example implementation:
/// ```rust
/// #[derive(Debug)]
/// pub struct ExampleTaskDispatcher<T> {
///     tasks: Arc<Mutex<Vec<T>>>,
/// }
///
/// impl<T> ExampleTaskDispatcher<T>
///     where
///         T: Task + MyExt,
/// {
///     fn new() -> ExampleTaskDispatcher<T> {
///         let tasks = arc!(std_mutex!(vec![]));
///         ExampleTaskDispatcher { tasks }
///     }
/// }
///
/// impl<T> Dispatcher<T> for ExampleTaskDispatcher<T>
///     where
///         T: Task + MyExt,
/// {
///     fn get_task(&self) -> Option<T> {
///         let mut guard = self.tasks.lock().unwrap();
///         let rtn_value = guard.pop();
///         rtn_value
///     }
///
///     fn add_task(&self, task: T) {
///         let mut guard = self.tasks.lock().unwrap();
///         dbg!(task.get_priority());
///         guard.push(task);
///     }
/// }
/// ```

pub trait Dispatcher<T: task::Task> {
    fn get_task(&self) -> Option<T>;
    fn add_task(&self, task: T);
}
