#[macro_export]
macro_rules! debug {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::debug!($($arg)+)
    };
}
pub(crate) use crate::debug;

#[macro_export]
macro_rules! warn {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::warn!($($arg)+)
    };
}
pub(crate) use crate::warn;

#[macro_export]
macro_rules! error {
    ($($arg:tt)+) => {
        #[cfg(test)]
        tracing::error!($($arg)+)
    };
}
pub(crate) use crate::error;
