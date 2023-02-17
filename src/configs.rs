/// Used by `CircuitHandler` to setup basic configuration.
pub struct CircuitHandlerConfig {
    /// How many tor clients to create.
    pub num_workers: usize,
    /// How long to sleep between requests (for `Circuit`).
    pub task_buffer: f32,
    /// How long to sleep if the `Circuit` has no requests.
    pub no_task_sleep: f32,
}

impl CircuitHandlerConfig {
    pub fn new(num_workers: usize, task_buffer: f32, no_task_sleep: f32) -> CircuitHandlerConfig {
        CircuitHandlerConfig {
            num_workers,
            task_buffer,
            no_task_sleep,
        }
    }
}
