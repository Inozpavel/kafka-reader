use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::Stream;

pin_project! {
    pub struct SteamWithData<S, D> {
        #[pin]
        stream: S,
        data: D,
    }
}

impl<T: Stream, D> Stream for SteamWithData<T, D> {
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

pub trait StreamDataExtension: Stream + Sized {
    fn with_data<D>(self, data: D) -> SteamWithData<Self, D> {
        SteamWithData { stream: self, data }
    }
}

impl<T: Stream + Sized> StreamDataExtension for T {}
