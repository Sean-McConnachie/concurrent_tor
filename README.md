# Concurrent tor

#### A library designed to run multiple tor instances concurrently.

#### User defined task-dispatcher allows for different queue designs, i.e. priority queue, binary heap etc...

See `/examples/basic.rs` for the boilerplate code to use this library.

### Task
Your implementation of a task. This library uses enum_delegate [enum_delegate](https://crates.io/crates/enum_delegate)
to allow for polymorphism. There is a `request::Task` trait which must be implemented. In addition to this, more traits
can be added to allow for more functionality. For example:

```rust
#[delegate]  // <-- Additional traits must be annotated with `#[delegate]`
pub trait MyExt {
    fn get_priority(&self) -> usize;
}

#[delegate(derive(Task, MyExt))]  // <-- Task enum must implement `Task`
pub enum ExampleTaskEnum {
    TaskOne(ExampleTaskOne),
    // TaskTwo(ExampleTaskTwo),
}

#[derive(Debug, Default)]
pub struct ExampleTaskOne {
    priority: usize,
    request: request::Request,
}

impl ExampleTaskOne {
    fn new(priority: usize, request: request::Request) -> ExampleTaskOne {
        ExampleTaskOne { priority, request }
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
```

### Task dispatcher
Your implementation of a queue/basic dispatcher.
```rust
pub struct ExampleTaskDispatcher<T> {
    tasks: Arc<Mutex<Vec<T>>>,
}

impl<T> ExampleTaskDispatcher<T>
where
    T: Task + MyExt,
{
    fn new() -> ExampleTaskDispatcher<T> {
        let tasks = arc!(Mutex::new(vec![]));
        ExampleTaskDispatcher { tasks }
    }
}

impl<T> dispatcher::Dispatcher<T> for ExampleTaskDispatcher<T>
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
```

### Request
Stores a request's:
 - `Uri`
 - `Headers`
 - `Method`
 - `Allow redirect`
 - `Maximum tries`
 - `Next attempt` -> `Standard`/`WebArchive`/`Ignore`

Along with some basic logic/functions.
