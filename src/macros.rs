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

#[cfg(test)]
pub(crate) mod test {
    #[macro_export]
    macro_rules! spawn {
        ($($code:tt)+) => {
            {
                #[cfg(feature = "loom")]
                let res = {
                    loom::thread::spawn(move || {
                        $($code)+
                    })
                };
                #[cfg(not(feature = "loom"))]
                let res = {
                    tokio::spawn(async move {
                        $($code)+
                    })
                };
                res
            }
        };
    }
    pub(crate) use crate::spawn;

    #[macro_export]
    macro_rules! block_on {
        ($($code:tt)+) => {
            #[cfg(feature = "loom")]
            {
                loom::future::block_on(async move {
                    $($code)+
                })
            }
            #[cfg(not(feature = "loom"))]
            {
                {
                    $($code)+
                };
                ()
            }
        }
    }
    pub(crate) use crate::block_on;

    #[macro_export]
    macro_rules! join {
        ($handle:ident) => {
            #[cfg(feature = "loom")]
            {
                $handle.join().unwrap();
            }
            #[cfg(not(feature = "loom"))]
            {
                $handle.await.unwrap();
            }
        };
    }
    pub(crate) use crate::join;
}
