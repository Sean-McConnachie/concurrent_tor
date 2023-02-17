#[macro_export]
macro_rules! bx {
    ($e:expr) => {
        Box::new($e)
    };
}

#[macro_export]
macro_rules! arc {
    ($e:expr) => {
        Arc::new($e)
    };
}

#[macro_export]
macro_rules! std_mutex {
    ($e:expr) => {
        std::sync::Mutex::new($e)
    };
}

#[macro_export]
macro_rules! tokio_mutex {
    ($e:expr) => {
        tokio::sync::Mutex::new($e)
    };
}

#[macro_export]
macro_rules! impl_task {
    () => {
        #[enum_delegate::implement(Task)]
    }
}
